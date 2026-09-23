//! The hub: agents push samples in on [`AGENT_PATH`], readers watch them on
//! [`UI_PATH`], and a small page at `/` draws them.
//!
//! State is memory only — the latest few minutes per host. Restarting the hub
//! loses history, and agents repopulate it within a tick of reconnecting.

use std::{
    collections::{HashMap, VecDeque},
    net::{IpAddr, SocketAddr},
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Json, Router,
    extract::{
        ConnectInfo, Request, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{Html, IntoResponse, Response},
    routing::get,
};
use fleetmon_proto::{
    AGENT_PATH, AGENT_SILENCE_MS, AgentMsg, HostInfo, HostView, Sample, UI_PATH, UiMsg,
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
/// Longest host name accepted in a hello.
const MAX_NAME: usize = 64;

pub struct Config {
    pub token: String,
    /// Sources allowed besides loopback. Empty means loopback only.
    pub allow: Vec<IpNet>,
    /// Samples kept per host.
    pub history: usize,
    /// Distinct host names kept. Together with `history` this bounds the hub's
    /// memory, whatever a token holder sends.
    pub max_hosts: usize,
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
    history: VecDeque<Sample>,
}

#[derive(Debug, PartialEq)]
pub enum Refused {
    BadName,
    Full,
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
            })
            .collect();
        views.sort_by(|a, b| a.info.name.cmp(&b.info.name));
        views
    }
}

impl Hub {
    pub fn new(cfg: Config) -> Self {
        let (events, _) = broadcast::channel(1024);
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
        if info.name.is_empty()
            || info.name.chars().count() > MAX_NAME
            || info.name.chars().any(char::is_control)
        {
            return Err(Refused::BadName);
        }
        let mut state = self.0.state.lock().unwrap();
        if !state.hosts.contains_key(&info.name) && state.hosts.len() >= self.0.cfg.max_hosts {
            // Make room by forgetting an offline host; never evict a live one.
            let stale = state
                .hosts
                .iter()
                .filter(|(_, e)| !e.online)
                .min_by_key(|(_, e)| e.history.back().map_or(0, |s| s.ts_ms))
                .map(|(name, _)| name.clone());
            let Some(stale) = stale else {
                return Err(Refused::Full);
            };
            state.hosts.remove(&stale);
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
            }
            None => {
                state.hosts.insert(
                    info.name.clone(),
                    Entry {
                        info: info.clone(),
                        conn,
                        online: true,
                        history: VecDeque::new(),
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
        if e.history.len() >= self.0.cfg.history {
            e.history.pop_front();
        }
        e.history.push_back(sample.clone());
        let _ = self.0.events.send(UiMsg::Sample {
            host: name.to_string(),
            sample,
        });
    }

    fn gone(&self, name: &str, conn: u64) {
        let mut state = self.0.state.lock().unwrap();
        let Some(e) = state.hosts.get_mut(name).filter(|e| e.conn == conn) else {
            return;
        };
        e.online = false;
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

async fn agent_ws(State(hub): State<Hub>, headers: HeaderMap, ws: WebSocketUpgrade) -> Response {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    if !bearer.is_some_and(|t| token_matches(t, &hub.0.cfg.token)) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    ws.max_message_size(MAX_AGENT_FRAME)
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
    let conn = match hub.hello(info) {
        Ok(conn) => conn,
        Err(why) => {
            tracing::warn!(host = %name, "hello refused: {why:?}");
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };

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
            ev = events.recv() => {
                let msg = match ev {
                    Ok(m) => m,
                    // A slow reader missed events; a fresh snapshot is cheaper
                    // than tracking what it missed. Resubscribing with it keeps
                    // the no-duplicates guarantee.
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let (fresh, hosts) = hub.subscribe();
                        events = fresh;
                        UiMsg::Snapshot { hosts }
                    }
                    Err(broadcast::error::RecvError::Closed) => return,
                };
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
        Hub::new(Config {
            token: "t".repeat(16),
            allow: vec![],
            history: 3,
            max_hosts,
        })
    }

    fn info(name: &str) -> HostInfo {
        HostInfo {
            name: name.into(),
            os: "os".into(),
            cpu_model: "cpu".into(),
            cores: 1,
            mem_total: 1,
            agent_version: "0".into(),
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
        assert_eq!(hub.hello(info("")), Err(Refused::BadName));
        assert_eq!(hub.hello(info(&"x".repeat(65))), Err(Refused::BadName));
        assert_eq!(hub.hello(info("a\nb")), Err(Refused::BadName));
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
        let s = Sample {
            ts_ms: 1,
            cpu_pct: 0.0,
            mem_used: 0,
            swap_used: 0,
            swap_total: 0,
            net_rx_bps: 0,
            net_tx_bps: 0,
            uptime_s: 0,
            top: vec![],
        };
        hub.sample("a", conn, s);
        assert!(matches!(rx.recv().await.unwrap(), UiMsg::Sample { .. }));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn token_comparison() {
        assert!(token_matches("abc", "abc"));
        assert!(!token_matches("abd", "abc"));
        assert!(!token_matches("ab", "abc"));
        assert!(!token_matches("", "abc"));
    }
}
