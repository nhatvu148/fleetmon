use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, bail};
use clap::Parser;

/// Push this machine's CPU, memory and network to a fleetmon hub.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Hub base URL, e.g. ws://100.64.0.1:7070 or wss://hub.example.com
    #[arg(long, env = "FLEETMON_HUB")]
    hub: String,
    /// File holding the shared token. Preferred over FLEETMON_TOKEN: it keeps
    /// the secret out of the environment of every child process and out of
    /// scheduled-task definitions. There is deliberately no --token flag, since
    /// a command line is visible to every user on the box.
    #[arg(long, env = "FLEETMON_TOKEN_FILE")]
    token_file: Option<PathBuf>,
    /// Name shown on the hub. Defaults to the hostname.
    #[arg(long, env = "FLEETMON_NAME")]
    name: Option<String>,
    /// Milliseconds between samples. Capped well under the hub's silence
    /// timeout, which would otherwise drop the agent between samples.
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u64).range(250..=fleetmon_proto::MAX_INTERVAL_MS))]
    interval_ms: u64,
    /// How many of the heaviest processes to include.
    #[arg(long, default_value_t = 5)]
    top: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();

    let token = match (&args.token_file, std::env::var("FLEETMON_TOKEN")) {
        (Some(path), _) => {
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?
        }
        (None, Ok(t)) => t,
        (None, Err(_)) => bail!("no token: pass --token-file or set FLEETMON_TOKEN"),
    };
    let token = token.trim().to_string();
    if token.is_empty() {
        bail!("token is empty");
    }

    let name = args
        .name
        .or_else(sysinfo::System::host_name)
        .context("no --name given and the hostname is unavailable")?;
    // The hub applies the same rule; failing here beats being refused on
    // every reconnect.
    if let Err(why) = fleetmon_proto::check_name(&name) {
        bail!("{why}: {name:?} (pass --name)");
    }

    fleetmon_agent::run(fleetmon_agent::Config {
        hub: args.hub,
        token,
        name,
        interval: Duration::from_millis(args.interval_ms),
        top: args.top,
    })
    .await
}
