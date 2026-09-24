//! Samples this machine and pushes each reading to a hub over one outbound
//! WebSocket. The agent never listens on a port: on a shared network, a box
//! that only dials out has nothing to probe.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use fleetmon_proto::{
    AGENT_PATH, AgentMsg, CLOSE_REFUSED, DiskInfo, HostInfo, Iface, Proc, Sample, Temp,
};
use futures_util::{SinkExt, StreamExt};
use sysinfo::{
    Components, CpuRefreshKind, DiskRefreshKind, Disks, Networks, ProcessRefreshKind,
    ProcessesToUpdate, System,
};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest, http::HeaderValue};

/// The slow readings — the process list, disk space and temperature sensors —
/// are taken on one tick in this many. Walking every process is by far the
/// most expensive part of a sample on Windows (about 9% of a core every second
/// on a busy desktop); CPU, memory, network and disk I/O stay per-tick and cheap.
/// Per-process CPU then averages over the longer window, which is steadier.
pub const PROC_EVERY: u64 = 5;

/// Interfaces reported per sample, busiest first.
const MAX_IFACES: usize = 6;
/// Disks reported per sample, largest first.
const MAX_DISKS: usize = 8;
/// Sensors reported per sample, hottest first.
const MAX_TEMPS: usize = 12;
/// Pseudo and virtual filesystems that would only add noise to a disk list.
const SKIP_FS: &[&str] = &[
    "tmpfs", "devtmpfs", "overlay", "squashfs", "autofs", "devfs", "nullfs",
];

/// Owns the sysinfo handles, because CPU, network and disk I/O figures are
/// deltas against the previous refresh and so must outlive a single reading.
pub struct Sampler {
    sys: System,
    nets: Networks,
    disks: Disks,
    sensors: Components,
    last: Instant,
    top: usize,
    ticks: u64,
    /// Results of the last slow refresh, reused until the next one.
    slow: Slow,
}

#[derive(Default)]
struct Slow {
    top: Vec<Proc>,
    top_mem: Vec<Proc>,
    proc_count: u32,
    /// Space per disk, keyed by mount point; I/O rates are filled per tick.
    space: Vec<(String, u64, u64)>,
    temps: Vec<Temp>,
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
            disks: Disks::new_with_refreshed_list(),
            sensors: Components::new_with_refreshed_list(),
            last: Instant::now(),
            top,
            ticks: 0,
            slow: Slow::default(),
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
            kernel: System::kernel_version().unwrap_or_default(),
            arch: System::cpu_arch(),
            physical_cores: System::physical_core_count(),
            boot_time: System::boot_time(),
        }
    }

    /// Takes one reading. Blocking: enumerating processes can take tens of
    /// milliseconds on Windows, so call it off the async runtime.
    pub fn sample(&mut self) -> Sample {
        self.sys.refresh_cpu_all();
        self.sys.refresh_memory();
        if self.ticks.is_multiple_of(PROC_EVERY) {
            self.refresh_slow();
        }
        self.ticks += 1;
        self.nets.refresh(true);
        self.disks
            .refresh_specifics(false, DiskRefreshKind::nothing().with_io_usage());

        let elapsed = self.last.elapsed().as_secs_f64().max(0.001);
        self.last = Instant::now();
        let rate = |bytes: u64| (bytes as f64 / elapsed) as u64;

        let mut ifaces: Vec<Iface> = self
            .nets
            .list()
            .iter()
            // Loopback and interfaces that have never moved a byte are noise.
            .filter(|(name, n)| {
                !name.starts_with("lo") && n.total_received() + n.total_transmitted() > 0
            })
            .map(|(name, n)| Iface {
                name: name.clone(),
                rx_bps: rate(n.received()),
                tx_bps: rate(n.transmitted()),
            })
            .collect();
        let (rx, tx) = ifaces
            .iter()
            .fold((0, 0), |(rx, tx), i| (rx + i.rx_bps, tx + i.tx_bps));
        // Show what is moving traffic now. A Mac or a Docker host carries a
        // dozen idle virtual interfaces (utun, vmenet, bridge, veth) that would
        // otherwise crowd out the one that matters.
        let lifetime = |name: &str| {
            self.nets
                .list()
                .get(name)
                .map_or(0, |n| n.total_received() + n.total_transmitted())
        };
        ifaces.sort_by_key(|i| std::cmp::Reverse((i.rx_bps + i.tx_bps, lifetime(&i.name))));
        let active = ifaces.iter().filter(|i| i.rx_bps + i.tx_bps > 0).count();
        ifaces.truncate(active.clamp(1, MAX_IFACES));

        let io: std::collections::HashMap<String, (u64, u64)> = self
            .disks
            .list()
            .iter()
            .map(|d| {
                let u = d.usage();
                (
                    d.mount_point().to_string_lossy().into_owned(),
                    (rate(u.read_bytes), rate(u.written_bytes)),
                )
            })
            .collect();
        let (disk_read_bps, disk_write_bps) = io
            .values()
            .fold((0, 0), |(r, w), (dr, dw)| (r + dr, w + dw));
        // Built from `space`, which is sorted largest first, so the first disk is
        // the main one — the overview card shows only that.
        let disks = self
            .slow
            .space
            .iter()
            .filter_map(|(mount, total, available)| {
                let d = self
                    .disks
                    .list()
                    .iter()
                    .find(|d| d.mount_point().to_string_lossy() == *mount)?;
                let (read_bps, write_bps) = io.get(mount).copied().unwrap_or_default();
                Some(DiskInfo {
                    name: d.name().to_string_lossy().into_owned(),
                    fs: d.file_system().to_string_lossy().into_owned(),
                    kind: d.kind().to_string(),
                    mount: mount.clone(),
                    total: *total,
                    available: *available,
                    read_bps,
                    write_bps,
                })
            })
            .collect();

        let cpus = self.sys.cpus();
        let cpu_freq_mhz =
            cpus.iter().map(|c| c.frequency()).sum::<u64>() / cpus.len().max(1) as u64;
        let load = System::load_average();

        Sample {
            ts_ms: now_ms(),
            cpu_pct: self.sys.global_cpu_usage(),
            mem_used: self.sys.used_memory(),
            swap_used: self.sys.used_swap(),
            swap_total: self.sys.total_swap(),
            net_rx_bps: rx,
            net_tx_bps: tx,
            uptime_s: System::uptime(),
            top: self.slow.top.clone(),
            cpu_cores: cpus.iter().map(|c| c.cpu_usage()).collect(),
            cpu_freq_mhz,
            // Windows has no load average; sysinfo reports zeros there.
            load_avg: (!cfg!(windows)).then_some([load.one, load.five, load.fifteen]),
            mem_available: self.sys.available_memory(),
            disk_read_bps,
            disk_write_bps,
            temp_max_c: self.slow.temps.first().map(|t| t.celsius),
            proc_count: self.slow.proc_count,
            top_mem: self.slow.top_mem.clone(),
            disks,
            ifaces,
            temps: self.slow.temps.clone(),
        }
    }

    fn refresh_slow(&mut self) {
        self.sys.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        let cores = self.sys.cpus().len().max(1) as f32;
        let mut procs: Vec<Proc> = self
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
        self.slow.proc_count = procs.len() as u32;
        procs.sort_by(|a, b| b.cpu_pct.total_cmp(&a.cpu_pct));
        self.slow.top = procs.iter().take(self.top).cloned().collect();
        procs.sort_by_key(|p| std::cmp::Reverse(p.mem));
        procs.truncate(self.top);
        self.slow.top_mem = procs;

        // New disks (a USB stick) appear on the list refresh; space changes slowly.
        self.disks
            .refresh_specifics(true, DiskRefreshKind::nothing().with_storage().with_kind());
        let mut space: Vec<(String, u64, u64)> = self
            .disks
            .list()
            .iter()
            .filter(|d| {
                let fs = d.file_system().to_string_lossy();
                let mount = d.mount_point().to_string_lossy();
                d.total_space() > 0
                    && !SKIP_FS.contains(&fs.as_ref())
                    // macOS mounts the same APFS container several times under
                    // /System/Volumes; "/" already stands for it.
                    && !mount.starts_with("/System/Volumes/")
                    && !mount.starts_with("/snap/")
                    && !mount.starts_with("/boot/efi")
            })
            .map(|d| {
                (
                    d.mount_point().to_string_lossy().into_owned(),
                    d.total_space(),
                    d.available_space(),
                )
            })
            .collect();
        space.sort_by_key(|s| std::cmp::Reverse(s.1));
        space.truncate(MAX_DISKS);
        self.slow.space = space;

        self.sensors.refresh(true);
        let mut temps: Vec<Temp> = self
            .sensors
            .list()
            .iter()
            // Apple Silicon exposes "PMU tcal", a fixed calibration constant, not a
            // temperature; it would otherwise sit at the top of the list forever.
            .filter(|c| !c.label().contains("tcal"))
            .filter_map(|c| {
                let celsius = c.temperature().filter(|t| t.is_finite() && *t > 0.0)?;
                Some(Temp {
                    label: c.label().to_string(),
                    celsius,
                    critical: c.critical().filter(|t| t.is_finite() && *t > 0.0),
                })
            })
            .collect();
        temps.sort_by(|a, b| b.celsius.total_cmp(&a.celsius));
        temps.truncate(MAX_TEMPS);
        self.slow.temps = temps;
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
            assert!(!s.slow.top.is_empty());
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
        assert_eq!(r.cpu_cores.len(), info.cores);
        assert!(r.mem_available > 0 && r.mem_available <= info.mem_total);
        // A container may have no real disk at all (overlay is filtered out), so
        // only the invariants are checked, not that a disk exists.
        assert!(r.disks.iter().all(|d| d.available <= d.total));
        assert!(
            r.disks.windows(2).all(|w| w[0].total >= w[1].total),
            "largest disk first"
        );
        assert!(r.proc_count > 0);
        assert!(r.top_mem.windows(2).all(|w| w[0].mem >= w[1].mem));
        assert!(
            !r.top.is_empty(),
            "the first sample must carry a process list"
        );
        assert!(info.cores > 0);
        assert!(r.mem_used <= info.mem_total);
    }
}
