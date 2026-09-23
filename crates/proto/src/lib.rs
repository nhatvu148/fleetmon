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

/// What a machine is. Sent once per connection, before any sample.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
}

/// One reading of a machine, taken every agent tick.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Oldest first. Bounded by the hub's history length.
    pub history: Vec<Sample>,
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
        }
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
