# fleetmon

Live CPU, memory, network and top processes across a handful of machines. A small Rust agent on each box pushes a sample every second to one hub, and the hub serves a page that draws them.

Built for the "few boxes I own" case — a laptop, a desktop, a cloud VM — where Prometheus and Grafana are more infrastructure than the problem.

```
 agent (Windows) ──┐
 agent (macOS)   ──┼── WebSocket, outbound only ──▶ hub ──▶ page at /
 agent (Linux)   ──┘                                 └──▶ /api/hosts (JSON)
```

## Design choices

- **Agents only dial out.** An agent never listens on a port, so on a shared network a monitored box exposes nothing new. Where the agent cannot reach the hub directly, a reverse SSH tunnel (`ssh -R 7070:127.0.0.1:7070 box`) lets it dial `127.0.0.1` instead.
- **One hub, one file.** Live data is held in memory; with `--db`, every sample also goes to SQLite (compiled in, no server): each second for 2 hours, then one average per minute for 31 days, which feeds the 1 h / 24 h / 7 d / 30 d charts. After a restart a reconnecting machine's recent history is reloaded, so charts continue. Without `--db` the hub keeps only the live window.
- **Two gates.** Agents present a bearer token. Every route — the page included, since it shows process names — is limited to loopback plus an `--allow` list of IPs or CIDRs.
- **No secrets on command lines.** Tokens come from a file (`--token-file`) or `FLEETMON_TOKEN`, never a flag, because a command line is visible to every user on the box.
- **No frontend build.** The page is one HTML file compiled into the hub binary, with hand-drawn canvas sparklines and no dependencies, so it works on a machine with no internet access.

## Quick start

```bash
cargo build --release
openssl rand -hex 24 > fleetmon.token

# hub, on the machine you watch from
target/release/fleetmon-hub --token-file fleetmon.token
#   → http://127.0.0.1:7070

# agent, on each machine (here: the same one)
target/release/fleetmon-agent --hub ws://127.0.0.1:7070 --token-file fleetmon.token
```

To accept agents from other machines, bind an address they can reach and allow them:

```bash
fleetmon-hub --bind 100.64.0.1:7070 --allow 100.64.0.0/10 --token-file fleetmon.token
```

To run the hub on a server behind a reverse proxy and TLS, see [docs/deploy.md](docs/deploy.md); agents then connect with `--hub wss://your.domain`.

## Running the agent as a service

- **macOS:** `task agent:install:macos HUB=wss://your.domain TOKEN_FILE=<path> [NAME=macbook]` builds the agent and installs a per-user LaunchAgent (starts at login, restarted if it exits; log in `~/Library/Logs/fleetmon-agent.log`). `task agent:uninstall:macos` removes it.
- **Windows:** a scheduled task running `fleetmon-agent.exe` at startup does the same job. The binary cross-compiles from macOS or Linux with `cargo zigbuild --release --target x86_64-pc-windows-gnu -p fleetmon-agent`.

## Options

| Hub | Env | Default | |
|---|---|---|---|
| `--bind` | `FLEETMON_BIND` | `127.0.0.1:7070` | listen address |
| `--allow` | `FLEETMON_ALLOW` | *(loopback only)* | comma-separated IPs/CIDRs |
| `--token-file` | `FLEETMON_TOKEN_FILE` | | or `FLEETMON_TOKEN`; at least 16 characters |
| `--history` | | `300` | samples kept per host |
| `--max-hosts` | | `64` | distinct hosts kept; when full, an offline host is forgotten to make room |
| `--db` | `FLEETMON_DB` | *(none)* | SQLite file for history; enables the 1 h – 30 d ranges |

| Agent | Env | Default | |
|---|---|---|---|
| `--hub` | `FLEETMON_HUB` | | e.g. `ws://100.64.0.1:7070` |
| `--token-file` | `FLEETMON_TOKEN_FILE` | | or `FLEETMON_TOKEN` |
| `--name` | `FLEETMON_NAME` | hostname | must be unique across the fleet |
| `--interval-ms` | | `1000` | 250 to 10000 (the hub drops an agent after 15 s of silence) |
| `--top` | | `5` | heaviest processes to report; the process list is re-read every 5th sample, the costliest part of sampling on Windows |

Logging follows `RUST_LOG` (default `info`).

## Layout

| Crate | |
|---|---|
| `crates/proto` | wire types shared by both sides — JSON, tagged by `type` |
| `crates/agent` | `fleetmon-agent`: [`sysinfo`](https://crates.io/crates/sysinfo) sampler plus a reconnecting WebSocket client |
| `crates/hub` | `fleetmon-hub`: axum server, in-memory state, the page |

## Roadmap

- An MCP tool on the hub (`machine_load`, `idlest_host`), so a coding agent can check whether a box is busy before sending it a long build.
- GPU utilisation on NVIDIA boxes.
- Optional on-disk history.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
