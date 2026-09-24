//! The hub: agents push samples in on [`AGENT_PATH`], readers watch them on
//! [`UI_PATH`], and a small page at `/` draws them.
//!
//! State is memory only — the latest few minutes per host. Restarting the hub
//! loses history, and agents repopulate it within a tick of reconnecting.

pub mod store;

use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    extract::{
        ConnectInfo, Query, Request, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use fleetmon_proto::{
    AGENT_PATH, AGENT_SILENCE_MS, AgentMsg, CLOSE_REFUSED, HostInfo, HostView, Sample, UI_PATH,
    UiMsg,
};
use futures_util::{SinkExt, StreamExt};
use ipnet::IpNet;
use tokio::sync::broadcast;

/// An agent that sends nothing for this long is treated as gone. Covers the
/// case a close frame never arrives: a sleeping laptop, a dropped tunnel.
const AGENT_SILENCE: Duration = Duration::from_millis(AGENT_SILENCE_MS);
/// A hello arrives immediately on a real agent; anything slower is not one.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// A sample with a few top processes is about 1 KB.
const MAX_AGENT_FRAME: usize = 64 * 1024;
/// Readers send nothing meaningful; this only bounds what they can make us buffer.
const MAX_UI_FRAME: usize = 4 * 1024;

pub struct Config {
    pub token: String,
    /// Sources allowed besides loopback. Empty means loopback only.
    pub allow: Vec<IpNet>,
    /// Samples kept per host.
    pub history: usize,
    /// Distinct host names kept. Together with `history` this bounds the hub's
    /// memory, whatever a token holder sends.
    pub max_hosts: usize,
    /// Durable history for the 1 h – 30 d ranges. Without it the hub serves
    /// only the live window it holds in memory.
    pub store: Option<store::Store>,
}

#[derive(Clone)]
pub struct Hub(Arc<Inner>);

struct Inner {
    cfg: Config,
    /// Every event is published while this lock is held, and a reader
    /// subscribes and snapshots under it too. That makes "in the snapshot" and
    /// "arrives as an event" mutually exclusive, so the page never sees one
    /// sample twice.
    state: Mutex<HubState>,
    events: broadcast::Sender<UiMsg>,
}

#[derive(Default)]
struct HubState {
    hosts: HashMap<String, Entry>,
    next_conn: u64,
}

struct Entry {
    info: HostInfo,
    /// Which connection currently owns this name. An agent that reconnects
    /// before the old socket times out must not be marked offline when that
    /// old socket finally drops.
    conn: u64,
    online: bool,
    /// When the hub last heard from this host, by the hub's clock. Eviction
    /// orders by this, never by the agent's own timestamps.
    last_seen: Instant,
    /// Slimmed samples: the scalars the charts plot, without per-item lists.
    history: VecDeque<Sample>,
    /// The newest sample in full, for the detail view.
    latest: Option<Sample>,
}

#[derive(Debug, PartialEq)]
pub enum Refused {
    BadName(&'static str),
    Full,
}

impl Refused {
    /// Sent to the agent as the close reason.
    fn reason(&self) -> &'static str {
        match self {
            Refused::BadName(why) => why,
            Refused::Full => "hub is full and every known host is online",
        }
    }
}

impl HubState {
    fn views(&self) -> Vec<HostView> {
        let mut views: Vec<HostView> = self
            .hosts
            .values()
            .map(|e| HostView {
                info: e.info.clone(),
                online: e.online,
                history: e.history.iter().cloned().collect(),
                latest: e.latest.clone(),
            })
            .collect();
        views.sort_by(|a, b| a.info.name.cmp(&b.info.name));
        views
    }
}

impl Hub {
    pub fn new(cfg: Config) -> Self {
        Self::with_capacity(cfg, 1024)
    }

    /// `capacity` is how many events a slow reader may fall behind by before
    /// it is resynced with a snapshot.
    fn with_capacity(cfg: Config, capacity: usize) -> Self {
        let (events, _) = broadcast::channel(capacity);
        Self(Arc::new(Inner {
            cfg,
            state: Mutex::default(),
            events,
        }))
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/", get(index))
            .route(UI_PATH, get(ui_ws))
            .route(AGENT_PATH, get(agent_ws))
            .route("/api/hosts", get(api_hosts))
            .route("/api/history", get(api_history))
            .layer(middleware::from_fn_with_state(self.clone(), allowlist))
            .with_state(self.clone())
    }

    pub fn snapshot(&self) -> Vec<HostView> {
        self.0.state.lock().unwrap().views()
    }

    /// A receiver plus the state it starts from, taken atomically: every event
    /// the receiver yields happened after the snapshot.
    fn subscribe(&self) -> (broadcast::Receiver<UiMsg>, Vec<HostView>) {
        let state = self.0.state.lock().unwrap();
        (self.0.events.subscribe(), state.views())
    }

    /// Registers a connection under `info.name` and returns its id.
    pub fn hello(&self, info: HostInfo) -> Result<u64, Refused> {
        fleetmon_proto::check_name(&info.name).map_err(Refused::BadName)?;
        let mut state = self.0.state.lock().unwrap();
        if !state.hosts.contains_key(&info.name) && state.hosts.len() >= self.0.cfg.max_hosts {
            // Make room by forgetting an offline host; never evict a live one.
            let stale = state
                .hosts
                .iter()
                .filter(|(_, e)| !e.online)
                .min_by_key(|(_, e)| e.last_seen)
                .map(|(name, _)| name.clone());
            let Some(stale) = stale else {
                return Err(Refused::Full);
            };
            state.hosts.remove(&stale);
            let _ = self.0.events.send(UiMsg::Removed { host: stale });
        }
        state.next_conn += 1;
        let conn = state.next_conn;
        match state.hosts.get_mut(&info.name) {
            Some(e) => {
                if e.online {
                    tracing::warn!(host = %info.name, "name taken by a live connection; the newer one wins");
                }
                e.info = info.clone();
                e.conn = conn;
                e.online = true;
                e.last_seen = Instant::now();
            }
            None => {
                state.hosts.insert(
                    info.name.clone(),
                    Entry {
                        info: info.clone(),
                        conn,
                        online: true,
                        last_seen: Instant::now(),
                        history: VecDeque::new(),
                        latest: None,
                    },
                );
            }
        }
        let _ = self.0.events.send(UiMsg::Host { info });
        Ok(conn)
    }

    fn sample(&self, name: &str, conn: u64, sample: Sample) {
        let mut state = self.0.state.lock().unwrap();
        let Some(e) = state.hosts.get_mut(name).filter(|e| e.conn == conn) else {
            return;
        };
        e.last_seen = Instant::now();
        if e.history.len() >= self.0.cfg.history {
            e.history.pop_front();
        }
        e.history.push_back(sample.slim());
        e.latest = Some(sample.clone());
        if let Some(store) = &self.0.cfg.store {
            store.record(name, &sample);
        }
        let _ = self.0.events.send(UiMsg::Sample {
            host: name.to_string(),
            sample,
        });
    }

    /// Fills an empty in-memory history from the store — after a hub restart,
    /// so a reconnecting host's charts continue instead of starting blank.
    fn seed(&self, name: &str, conn: u64, samples: Vec<Sample>) {
        let mut state = self.0.state.lock().unwrap();
        let Some(e) = state.hosts.get_mut(name).filter(|e| e.conn == conn) else {
            return;
        };
        if !e.history.is_empty() || samples.is_empty() {
            return;
        }
        let keep = self.0.cfg.history;
        e.history = samples.into_iter().rev().take(keep).rev().collect();
        // Readers learn of it the same way as any change of shape: a snapshot.
        let hosts = state.views();
        let _ = self.0.events.send(UiMsg::Snapshot { hosts });
    }

    fn gone(&self, name: &str, conn: u64) {
        let mut state = self.0.state.lock().unwrap();
        let Some(e) = state.hosts.get_mut(name).filter(|e| e.conn == conn) else {
            return;
        };
        e.online = false;
        e.last_seen = Instant::now();
        let _ = self.0.events.send(UiMsg::Offline {
            host: name.to_string(),
        });
    }
}

/// Loopback is always allowed; anything else must match `--allow`. Applies to
/// every route, the page included — the page shows process names.
pub fn is_allowed(ip: IpAddr, allow: &[IpNet]) -> bool {
    let ip = ip.to_canonical();
    ip.is_loopback() || allow.iter().any(|net| net.contains(&ip))
}

async fn allowlist(
    State(hub): State<Hub>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Response {
    if is_allowed(peer.ip(), &hub.0.cfg.allow) {
        next.run(req).await
    } else {
        tracing::warn!(%peer, path = %req.uri().path(), "refused: not on the allowlist");
        StatusCode::FORBIDDEN.into_response()
    }
}

/// Compares in time independent of where the first differing byte is.
fn token_matches(given: &str, expected: &str) -> bool {
    let (a, b) = (given.as_bytes(), expected.as_bytes());
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn api_hosts(State(hub): State<Hub>) -> Json<Vec<HostView>> {
    Json(hub.snapshot())
}

#[derive(serde::Deserialize)]
struct HistoryQuery {
    host: String,
    range: String,
}

/// `5m` is the live window from memory; `1h`, `24h`, `7d` and `30d` come from
/// the store, averaged to at most [`store::POINTS`] points.
async fn api_history(State(hub): State<Hub>, Query(q): Query<HistoryQuery>) -> Response {
    if q.range == "5m" {
        let state = hub.0.state.lock().unwrap();
        return match state.hosts.get(&q.host) {
            Some(e) => Json(e.history.iter().cloned().collect::<Vec<_>>()).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        };
    }
    let Some(range) = store::Range::parse(&q.range) else {
        return (
            StatusCode::BAD_REQUEST,
            "range must be 5m, 1h, 24h, 7d or 30d",
        )
            .into_response();
    };
    if hub.0.cfg.store.is_none() {
        return (
            StatusCode::NOT_FOUND,
            "this hub keeps no history; start it with --db",
        )
            .into_response();
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let h = hub.clone();
    match tokio::task::spawn_blocking(move || {
        h.0.cfg
            .store
            .as_ref()
            .expect("checked above")
            .history(&q.host, range, now)
    })
    .await
    {
        Ok(Ok(points)) => Json(points).into_response(),
        Ok(Err(e)) => {
            tracing::warn!("history query failed: {e:#}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn agent_ws(State(hub): State<Hub>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if !bearer.is_some_and(|t| token_matches(t, &hub.0.cfg.token)) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    // Both caps: max_message_size is checked only after a whole frame has been
    // read, and a frame may otherwise be 16 MB.
    ws.max_message_size(MAX_AGENT_FRAME)
        .max_frame_size(MAX_AGENT_FRAME)
        .on_upgrade(move |socket| agent_session(hub, socket))
}

async fn agent_session(hub: Hub, mut socket: WebSocket) {
    let info = match tokio::time::timeout(HELLO_TIMEOUT, next_msg(&mut socket)).await {
        Ok(Some(AgentMsg::Hello(info))) => info,
        _ => {
            tracing::warn!("agent did not say hello; dropping");
            return;
        }
    };
    let name = info.name.clone();
    tracing::info!(host = %name, os = %info.os, "agent online");
    let seed = match hub.0.cfg.store.is_some() {
        true => {
            let (h, n, k) = (hub.clone(), name.clone(), hub.0.cfg.history);
            tokio::task::spawn_blocking(move || h.0.cfg.store.as_ref().map(|s| s.recent(&n, k)))
                .await
                .ok()
                .flatten()
                .and_then(|r| r.map_err(|e| tracing::warn!("reading history: {e:#}")).ok())
                .unwrap_or_default()
        }
        false => Vec::new(),
    };
    let conn = match hub.hello(info) {
        Ok(conn) => conn,
        Err(why) => {
            tracing::warn!(host = %name, "hello refused: {}", why.reason());
            let _ = socket
                .send(Message::Close(Some(CloseFrame {
                    code: CLOSE_REFUSED,
                    reason: why.reason().into(),
                })))
                .await;
            return;
        }
    };
    hub.seed(&name, conn, seed);

    loop {
        match tokio::time::timeout(AGENT_SILENCE, next_msg(&mut socket)).await {
            Ok(Some(AgentMsg::Sample(s))) => hub.sample(&name, conn, s),
            Ok(Some(AgentMsg::Hello(_))) => tracing::warn!(host = %name, "second hello ignored"),
            Ok(None) => break,
            Err(_) => {
                tracing::warn!(host = %name, "silent for {AGENT_SILENCE:?}; dropping");
                break;
            }
        }
    }
    tracing::info!(host = %name, "agent offline");
    hub.gone(&name, conn);
}

/// Next parseable agent message, skipping control frames and garbage. `None`
/// once the socket is closed.
async fn next_msg(socket: &mut WebSocket) -> Option<AgentMsg> {
    loop {
        match socket.recv().await? {
            Ok(Message::Text(t)) => match serde_json::from_str(&t) {
                Ok(m) => return Some(m),
                Err(e) => tracing::warn!("unparseable agent frame: {e}"),
            },
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

async fn ui_ws(State(hub): State<Hub>, ws: WebSocketUpgrade) -> Response {
    ws.max_message_size(MAX_UI_FRAME)
        .max_frame_size(MAX_UI_FRAME)
        .on_upgrade(move |socket| ui_session(hub, socket))
}

async fn ui_session(hub: Hub, socket: WebSocket) {
    let (mut tx, mut rx) = socket.split();
    let (mut events, hosts) = hub.subscribe();
    if send(&mut tx, &UiMsg::Snapshot { hosts }).await.is_err() {
        return;
    }

    loop {
        tokio::select! {
            msg = next_event(&hub, &mut events) => {
                let Some(msg) = msg else { return };
                if send(&mut tx, &msg).await.is_err() {
                    return;
                }
            }
            // Readers have nothing to say; this only notices them leaving.
            frame = rx.next() => match frame {
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                Some(Ok(_)) => {}
            },
        }
    }
}

/// The next thing to tell a reader. `None` once the hub is shutting down.
async fn next_event(hub: &Hub, events: &mut broadcast::Receiver<UiMsg>) -> Option<UiMsg> {
    match events.recv().await {
        Ok(m) => Some(m),
        // A slow reader missed events; a fresh snapshot is cheaper than
        // tracking what it missed. Resubscribing with it keeps the
        // no-duplicates guarantee.
        Err(broadcast::error::RecvError::Lagged(_)) => {
            let (fresh, hosts) = hub.subscribe();
            *events = fresh;
            Some(UiMsg::Snapshot { hosts })
        }
        Err(broadcast::error::RecvError::Closed) => None,
    }
}

async fn send(
    tx: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    msg: &UiMsg,
) -> Result<(), axum::Error> {
    let json = serde_json::to_string(msg).expect("UiMsg always serializes");
    tx.send(Message::Text(json.into())).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_always_allowed_others_need_a_match() {
        let allow: Vec<IpNet> = vec!["100.64.0.0/10".parse().unwrap()];
        assert!(is_allowed("127.0.0.1".parse().unwrap(), &[]));
        assert!(is_allowed("::1".parse().unwrap(), &[]));
        assert!(is_allowed("100.100.1.2".parse().unwrap(), &allow));
        assert!(!is_allowed("192.168.1.5".parse().unwrap(), &allow));
        assert!(!is_allowed("100.100.1.2".parse().unwrap(), &[]));
    }

    #[test]
    fn ipv4_mapped_ipv6_is_treated_as_ipv4() {
        let allow: Vec<IpNet> = vec!["100.64.0.0/10".parse().unwrap()];
        assert!(is_allowed("::ffff:100.100.1.2".parse().unwrap(), &allow));
        assert!(is_allowed("::ffff:127.0.0.1".parse().unwrap(), &[]));
    }

    fn hub(max_hosts: usize) -> Hub {
        hub_with_capacity(max_hosts, 1024)
    }

    fn hub_with_capacity(max_hosts: usize, capacity: usize) -> Hub {
        Hub::with_capacity(
            Config {
                token: "t".repeat(16),
                allow: vec![],
                history: 3,
                max_hosts,
                store: None,
            },
            capacity,
        )
    }

    fn reading(ts_ms: u64) -> Sample {
        Sample {
            ts_ms,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn eviction_takes_the_longest_silent_and_tells_readers() {
        let hub = hub(2);
        let a = hub.hello(info("a")).unwrap();
        let b = hub.hello(info("b")).unwrap();
        // "a" claims the latest time by its own clock, but the hub heard from
        // it longest ago. Only the hub's clock may decide.
        hub.sample("a", a, reading(u64::MAX));
        hub.gone("a", a);
        std::thread::sleep(Duration::from_millis(5));
        hub.gone("b", b);

        let (mut rx, _) = hub.subscribe();
        hub.hello(info("c")).unwrap();
        assert_eq!(
            rx.recv().await.unwrap(),
            UiMsg::Removed { host: "a".into() }
        );
        let names: Vec<_> = hub.snapshot().into_iter().map(|v| v.info.name).collect();
        assert_eq!(names, ["b", "c"]);
    }

    #[tokio::test]
    async fn a_lagging_reader_gets_one_snapshot_then_only_newer_events() {
        let hub = hub_with_capacity(8, 2);
        let conn = hub.hello(info("a")).unwrap();
        let (mut rx, _) = hub.subscribe();
        for t in 1..=5 {
            hub.sample("a", conn, reading(t));
        }
        match next_event(&hub, &mut rx).await.unwrap() {
            UiMsg::Snapshot { hosts } => {
                let ts: Vec<_> = hosts[0].history.iter().map(|s| s.ts_ms).collect();
                assert_eq!(ts, [3, 4, 5]);
            }
            other => panic!("expected snapshot, got {other:?}"),
        }
        hub.sample("a", conn, reading(6));
        match next_event(&hub, &mut rx).await.unwrap() {
            UiMsg::Sample { sample, .. } => assert_eq!(sample.ts_ms, 6),
            other => panic!("expected sample, got {other:?}"),
        }
        assert!(rx.try_recv().is_err());
    }

    fn info(name: &str) -> HostInfo {
        HostInfo {
            name: name.into(),
            os: "os".into(),
            cpu_model: "cpu".into(),
            cores: 1,
            mem_total: 1,
            agent_version: "0".into(),
            ..Default::default()
        }
    }

    #[test]
    fn host_cap_evicts_offline_but_never_live() {
        let hub = hub(2);
        let a = hub.hello(info("a")).unwrap();
        hub.hello(info("b")).unwrap();
        assert_eq!(hub.hello(info("c")), Err(Refused::Full));
        // A known name is not a new host, so it is never refused for space.
        assert!(hub.hello(info("b")).is_ok());

        hub.gone("a", a);
        hub.hello(info("c")).unwrap();
        let names: Vec<_> = hub.snapshot().into_iter().map(|v| v.info.name).collect();
        assert_eq!(names, ["b", "c"]);
    }

    #[test]
    fn bad_names_are_refused() {
        let hub = hub(8);
        for bad in [String::new(), "x".repeat(65), "a\nb".into()] {
            assert!(
                matches!(hub.hello(info(&bad)), Err(Refused::BadName(_))),
                "{bad:?}"
            );
        }
        assert!(hub.hello(info(&"x".repeat(64))).is_ok());
    }

    #[test]
    fn stale_connection_cannot_touch_a_reclaimed_name() {
        let hub = hub(8);
        let old = hub.hello(info("a")).unwrap();
        let new = hub.hello(info("a")).unwrap();
        assert!(new > old);
        hub.gone("a", old);
        assert!(
            hub.snapshot()[0].online,
            "old socket marked the new one offline"
        );
    }

    #[tokio::test]
    async fn events_after_subscribe_are_not_in_the_snapshot() {
        let hub = hub(8);
        let conn = hub.hello(info("a")).unwrap();
        let (mut rx, snap) = hub.subscribe();
        assert!(snap[0].history.is_empty());
        hub.sample("a", conn, reading(1));
        assert!(matches!(rx.recv().await.unwrap(), UiMsg::Sample { .. }));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn history_is_slim_and_latest_is_full() {
        let hub = hub(8);
        let conn = hub.hello(info("a")).unwrap();
        let full = Sample {
            ts_ms: 9,
            top_mem: vec![fleetmon_proto::Proc {
                pid: 1,
                name: "p".into(),
                cpu_pct: 0.0,
                mem: 1,
            }],
            cpu_cores: vec![1.0, 2.0],
            ..Default::default()
        };
        hub.sample("a", conn, full.clone());
        let view = &hub.snapshot()[0];
        assert!(view.history[0].top_mem.is_empty() && view.history[0].cpu_cores.is_empty());
        assert_eq!(view.latest.as_ref(), Some(&full));
    }

    #[test]
    fn token_comparison() {
        assert!(token_matches("abc", "abc"));
        assert!(!token_matches("abd", "abc"));
        assert!(!token_matches("ab", "abc"));
        assert!(!token_matches("", "abc"));
    }
}
