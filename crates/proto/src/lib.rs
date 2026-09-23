//! Wire types shared by the agent and the hub.
//!
//! Everything travels as JSON text frames over a WebSocket. Both enums are
//! internally tagged (`{"type": "sample", ...}`) so the browser page can switch
//! on one field without knowing Rust's enum layout.

use serde::{Deserialize, Serialize};

/// Path the agent connects to on the hub.
pub const AGENT_PATH: &str = "/agent";
/// Path the browser page connects to on the hub.
pub const UI_PATH: &str = "/ws";
/// Longest gap an agent may leave between samples. The hub drops an agent
/// after [`AGENT_SILENCE_MS`], so this must stay below it with room for a slow
/// sample.
pub const MAX_INTERVAL_MS: u64 = 10_000;
/// How long the hub waits for anything from an agent before calling it gone.
pub const AGENT_SILENCE_MS: u64 = MAX_INTERVAL_MS + 5_000;
/// Longest host name a hub accepts, in characters.
pub const MAX_NAME: usize = 64;
/// WebSocket close code a hub sends when it refuses a hello (1008, policy
/// violation). The close reason says why.
pub const CLOSE_REFUSED: u16 = 1008;

/// The hub's rule for host names, shared so an agent can reject a bad `--name`
/// at startup instead of being refused on every reconnect.
pub fn check_name(name: &str) -> Result<(), &'static str> {
    if name.is_empty() {
        Err("host name is empty")
    } else if name.chars().count() > MAX_NAME {
        Err("host name is longer than 64 characters")
    } else if name.chars().any(char::is_control) {
        Err("host name contains a control character")
    } else {
        Ok(())
    }
}

/// What a machine is. Sent once per connection, before any sample.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct HostInfo {
    /// Display name, unique across the fleet. Defaults to the hostname.
    pub name: String,
    /// e.g. "Windows 11 Pro 24H2", "macOS 26.0".
    pub os: String,
    pub cpu_model: String,
    /// Logical cores.
    pub cores: usize,
    pub mem_total: u64,
    pub agent_version: String,
    // Everything below arrived after 0.1.0 and defaults when absent, so agents
    // and hubs of different versions keep talking to each other.
    /// e.g. "24.6.0" (Darwin), "10.0.26100" (Windows NT).
    #[serde(default)]
    pub kernel: String,
    /// e.g. "arm64", "x86_64".
    #[serde(default)]
    pub arch: String,
    #[serde(default)]
    pub physical_cores: Option<usize>,
    /// Unix seconds.
    #[serde(default)]
    pub boot_time: u64,
}

/// One reading of a machine, taken every agent tick.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    /// Unix milliseconds, from the agent's clock.
    pub ts_ms: u64,
    /// Whole-machine CPU, 0-100.
    pub cpu_pct: f32,
    /// Bytes.
    pub mem_used: u64,
    pub swap_used: u64,
    pub swap_total: u64,
    /// Bytes per second across every interface, since the previous sample.
    pub net_rx_bps: u64,
    pub net_tx_bps: u64,
    pub uptime_s: u64,
    /// Heaviest processes by CPU, heaviest first.
    pub top: Vec<Proc>,
    // Added after 0.1.0; see HostInfo.
    /// Per logical core, 0-100.
    #[serde(default)]
    pub cpu_cores: Vec<f32>,
    /// Mean current clock across cores.
    #[serde(default)]
    pub cpu_freq_mhz: u64,
    /// 1, 5 and 15 minute load average. Not available on Windows.
    #[serde(default)]
    pub load_avg: Option<[f64; 3]>,
    /// Memory an allocation could get without swapping, which on macOS and
    /// Linux is far more than "free".
    #[serde(default)]
    pub mem_available: u64,
    /// Bytes per second across every disk.
    #[serde(default)]
    pub disk_read_bps: u64,
    #[serde(default)]
    pub disk_write_bps: u64,
    /// Hottest sensor, for the history chart. `None` where the OS exposes none.
    #[serde(default)]
    pub temp_max_c: Option<f32>,
    #[serde(default)]
    pub proc_count: u32,
    /// Heaviest processes by memory, heaviest first.
    #[serde(default)]
    pub top_mem: Vec<Proc>,
    #[serde(default)]
    pub disks: Vec<DiskInfo>,
    #[serde(default)]
    pub ifaces: Vec<Iface>,
    #[serde(default)]
    pub temps: Vec<Temp>,
}

impl Sample {
    /// The sample without its per-item lists — what a hub keeps for history.
    /// Charts need the scalars over time; lists like disks and processes are
    /// only ever shown for the latest sample, and they are most of the size.
    pub fn slim(&self) -> Sample {
        Sample {
            top: Vec::new(),
            cpu_cores: Vec::new(),
            top_mem: Vec::new(),
            disks: Vec::new(),
            ifaces: Vec::new(),
            temps: Vec::new(),
            ..self.clone()
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiskInfo {
    pub name: String,
    pub mount: String,
    pub fs: String,
    /// "SSD", "HDD" or "Unknown".
    pub kind: String,
    pub total: u64,
    pub available: u64,
    pub read_bps: u64,
    pub write_bps: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Iface {
    pub name: String,
    pub rx_bps: u64,
    pub tx_bps: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Temp {
    pub label: String,
    pub celsius: f32,
    #[serde(default)]
    pub critical: Option<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Proc {
    pub pid: u32,
    pub name: String,
    /// Share of the whole machine, 0-100 — already divided by core count, so it
    /// is on the same scale as [`Sample::cpu_pct`].
    pub cpu_pct: f32,
    pub mem: u64,
}

/// Agent → hub.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AgentMsg {
    Hello(HostInfo),
    Sample(Sample),
}

/// Everything the hub knows about one host.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HostView {
    pub info: HostInfo,
    pub online: bool,
    /// Oldest first, slimmed (see [`Sample::slim`]). Bounded by the hub's
    /// history length.
    pub history: Vec<Sample>,
    /// The most recent sample in full, lists included.
    #[serde(default)]
    pub latest: Option<Sample>,
}

/// Hub → browser (and any other reader of `/ws`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum UiMsg {
    /// Sent once when a reader connects.
    Snapshot {
        hosts: Vec<HostView>,
    },
    /// A host connected (or reconnected) and said hello.
    Host {
        info: HostInfo,
    },
    Sample {
        host: String,
        sample: Sample,
    },
    Offline {
        host: String,
    },
    /// The hub forgot a host to make room for a new one.
    Removed {
        host: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Sample {
        Sample {
            ts_ms: 1,
            cpu_pct: 12.5,
            mem_used: 2,
            swap_used: 0,
            swap_total: 0,
            net_rx_bps: 3,
            net_tx_bps: 4,
            uptime_s: 5,
            top: vec![Proc {
                pid: 7,
                name: "cargo".into(),
                cpu_pct: 50.0,
                mem: 9,
            }],
            cpu_cores: vec![10.0, 15.0],
            cpu_freq_mhz: 3200,
            load_avg: Some([1.0, 0.5, 0.25]),
            mem_available: 1,
            disk_read_bps: 6,
            disk_write_bps: 7,
            temp_max_c: Some(55.0),
            proc_count: 300,
            top_mem: vec![],
            disks: vec![],
            ifaces: vec![],
            temps: vec![],
        }
    }

    #[test]
    fn a_010_agent_sample_still_parses() {
        // What a 0.1.0 agent sends: none of the later fields.
        let old = r#"{"type":"sample","ts_ms":1,"cpu_pct":1.0,"mem_used":2,"swap_used":0,
            "swap_total":0,"net_rx_bps":0,"net_tx_bps":0,"uptime_s":3,"top":[]}"#;
        let AgentMsg::Sample(s) = serde_json::from_str(old).unwrap() else {
            panic!("not a sample")
        };
        assert!(s.cpu_cores.is_empty() && s.load_avg.is_none() && s.disks.is_empty());
    }

    #[test]
    fn slim_keeps_scalars_and_drops_lists() {
        let s = sample().slim();
        assert_eq!(s.cpu_pct, 12.5);
        assert_eq!(s.temp_max_c, Some(55.0));
        assert!(s.top.is_empty() && s.cpu_cores.is_empty());
    }

    #[test]
    fn agent_msg_is_tagged_and_round_trips() {
        let msg = AgentMsg::Sample(sample());
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.starts_with(r#"{"type":"sample""#), "{json}");
        assert_eq!(serde_json::from_str::<AgentMsg>(&json).unwrap(), msg);
    }

    #[test]
    fn ui_msg_is_tagged_and_round_trips() {
        let msg = UiMsg::Offline {
            host: "box-a".into(),
        };
        let json = serde_json::to_string(&msg).unwrap();
        assert_eq!(json, r#"{"type":"offline","host":"box-a"}"#);
        assert_eq!(serde_json::from_str::<UiMsg>(&json).unwrap(), msg);
    }
}
