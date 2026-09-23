//! Samples this machine and pushes each reading to a hub over one outbound
//! WebSocket. The agent never listens on a port: on a shared network, a box
//! that only dials out has nothing to probe.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use fleetmon_proto::{AGENT_PATH, AgentMsg, CLOSE_REFUSED, HostInfo, Proc, Sample};
use futures_util::{SinkExt, StreamExt};
use sysinfo::{CpuRefreshKind, Networks, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest, http::HeaderValue};

/// The process list is re-read on one tick in this many. Walking every process
/// is by far the most expensive part of a sample on Windows — about 9% of a core
/// every second on a busy desktop — while CPU, memory and network stay cheap.
/// Per-process CPU then averages over the longer window, which is steadier.
pub const PROC_EVERY: u64 = 5;

/// Owns the sysinfo handles, because CPU and network figures are deltas
/// against the previous refresh and so must outlive a single reading.
pub struct Sampler {
    sys: System,
    nets: Networks,
    last: Instant,
    top: usize,
    ticks: u64,
    /// Heaviest processes as of the last process refresh.
    top_cache: Vec<Proc>,
}

impl Sampler {
    pub fn new(top: usize) -> Self {
        let mut sys = System::new();
        sys.refresh_cpu_list(CpuRefreshKind::everything());
        sys.refresh_memory();
        // Primes the per-process CPU baseline; the first real sample is a delta
        // against this one.
        sys.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        Self {
            sys,
            nets: Networks::new_with_refreshed_list(),
            last: Instant::now(),
            top,
            ticks: 0,
            top_cache: Vec::new(),
        }
    }

    pub fn host_info(&self, name: String) -> HostInfo {
        let os = System::long_os_version()
            .or_else(System::name)
            .unwrap_or_else(|| std::env::consts::OS.to_string());
        HostInfo {
            name,
            os,
            cpu_model: self
                .sys
                .cpus()
                .first()
                .map(|c| c.brand().trim().to_string())
                .unwrap_or_default(),
            cores: self.sys.cpus().len(),
            mem_total: self.sys.total_memory(),
            agent_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }

    /// Takes one reading. Blocking: enumerating processes can take tens of
    /// milliseconds on Windows, so call it off the async runtime.
    pub fn sample(&mut self) -> Sample {
        self.sys.refresh_cpu_usage();
        self.sys.refresh_memory();
        if self.ticks.is_multiple_of(PROC_EVERY) {
            self.refresh_top();
        }
        self.ticks += 1;
        self.nets.refresh(true);

        let elapsed = self.last.elapsed().as_secs_f64().max(0.001);
        self.last = Instant::now();
        let (rx, tx) = self.nets.list().values().fold((0u64, 0u64), |(rx, tx), n| {
            (rx + n.received(), tx + n.transmitted())
        });

        Sample {
            ts_ms: now_ms(),
            cpu_pct: self.sys.global_cpu_usage(),
            mem_used: self.sys.used_memory(),
            swap_used: self.sys.used_swap(),
            swap_total: self.sys.total_swap(),
            net_rx_bps: (rx as f64 / elapsed) as u64,
            net_tx_bps: (tx as f64 / elapsed) as u64,
            uptime_s: System::uptime(),
            top: self.top_cache.clone(),
        }
    }

    fn refresh_top(&mut self) {
        self.sys.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        let cores = self.sys.cpus().len().max(1) as f32;
        let mut top: Vec<Proc> = self
            .sys
            .processes()
            .values()
            .map(|p| Proc {
                pid: p.pid().as_u32(),
                name: p.name().to_string_lossy().into_owned(),
                cpu_pct: p.cpu_usage() / cores,
                mem: p.memory(),
            })
            .collect();
        top.sort_by(|a, b| b.cpu_pct.total_cmp(&a.cpu_pct));
        top.truncate(self.top);
        self.top_cache = top;
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub struct Config {
    /// Hub base URL: `ws://100.64.0.1:7070` on a private network, or
    /// `wss://hub.example.com` behind TLS. [`AGENT_PATH`] is appended.
    pub hub: String,
    pub token: String,
    pub name: String,
    pub interval: Duration,
    pub top: usize,
}

/// Connects, streams samples, and reconnects with backoff forever. Returns only
/// on an error that retrying cannot fix, such as a malformed hub URL.
pub async fn run(cfg: Config) -> Result<()> {
    // Needed before the first wss:// connect. Errors only if a provider is
    // already installed, which is fine.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let url = format!("{}{}", cfg.hub.trim_end_matches('/'), AGENT_PATH);
    // Held across connections so CPU and network deltas carry over. It is `None`
    // only if a sample panicked mid-flight; `stream` then builds a fresh one
    // rather than taking the whole agent down with it.
    let mut sampler = None;
    let mut backoff = Duration::from_secs(1);

    loop {
        let mut request = url.as_str().into_client_request().context("hub URL")?;
        let auth = HeaderValue::from_str(&format!("Bearer {}", cfg.token))
            .context("token is not a valid header value")?;
        request.headers_mut().insert("authorization", auth);

        match tokio_tungstenite::connect_async(request).await {
            Ok((ws, _)) => {
                tracing::info!(%url, "connected");
                match stream(ws, &mut sampler, &mut backoff, &cfg).await {
                    Ok(()) => tracing::warn!("hub closed the connection"),
                    Err(e) => tracing::warn!("connection lost: {e:#}"),
                }
            }
            // A 401 will not fix itself, but the operator may be about to fix
            // the hub's token, so keep retrying at the capped interval.
            Err(e) => tracing::warn!(%url, "connect failed: {e}"),
        }

        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Streams until the connection ends. Resets `backoff` only once a sample has
/// been sent: a hub that accepts the socket and then refuses the hello must not
/// be retried every second.
async fn stream(
    ws: Ws,
    sampler: &mut Option<Sampler>,
    backoff: &mut Duration,
    cfg: &Config,
) -> Result<()> {
    let (mut tx, mut rx) = ws.split();
    let info = sampler
        .get_or_insert_with(|| Sampler::new(cfg.top))
        .host_info(cfg.name.clone());
    tx.send(Message::text(serde_json::to_string(&AgentMsg::Hello(
        info,
    ))?))
    .await?;

    let mut tick = tokio::time::interval(cfg.interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await; // the first tick is immediate; the CPU delta needs a gap

    loop {
        tokio::select! {
            _ = tick.tick() => {
                let mut s = sampler.take().unwrap_or_else(|| Sampler::new(cfg.top));
                // If this panics the sampler is lost with it, `?` ends the
                // connection, and the next one starts with a new sampler.
                let (s, reading) = tokio::task::spawn_blocking(move || {
                    let reading = s.sample();
                    (s, reading)
                })
                .await
                .context("sampling panicked")?;
                *sampler = Some(s);
                tx.send(Message::text(serde_json::to_string(&AgentMsg::Sample(reading))?)).await?;
                *backoff = Duration::from_secs(1);
            }
            frame = rx.next() => match frame {
                // Pings are answered by tungstenite on the next write; nothing
                // else is expected from the hub.
                Some(Ok(Message::Close(Some(f)))) if u16::from(f.code) == CLOSE_REFUSED => {
                    bail!("hub refused this agent: {}", f.reason)
                }
                Some(Ok(Message::Close(_))) | None => return Ok(()),
                Some(Ok(_)) => {}
                Some(Err(e)) => bail!(e),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_list_is_refreshed_only_every_proc_every_ticks() {
        let mut s = Sampler::new(3);
        let first = s.sample();
        for _ in 1..PROC_EVERY {
            // Between refreshes the list is reused as-is, not re-derived.
            assert_eq!(s.sample().top, first.top);
        }
        s.sample();
        assert_eq!(s.ticks, PROC_EVERY + 1);
    }

    #[test]
    fn sample_is_sane() {
        let mut s = Sampler::new(3);
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        let r = s.sample();
        assert!((0.0..=100.0).contains(&r.cpu_pct), "cpu {}", r.cpu_pct);
        assert!(r.mem_used > 0);
        assert!(r.top.len() <= 3);
        assert!(r.top.windows(2).all(|w| w[0].cpu_pct >= w[1].cpu_pct));

        let info = s.host_info("test".into());
        assert!(
            !r.top.is_empty(),
            "the first sample must carry a process list"
        );
        assert!(info.cores > 0);
        assert!(r.mem_used <= info.mem_total);
    }
}
