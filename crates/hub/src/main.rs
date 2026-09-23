use std::{net::SocketAddr, path::PathBuf};

use anyhow::{Context, Result, bail};
use clap::Parser;
use ipnet::IpNet;

/// Collect samples from fleetmon agents and serve a live page.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Address to listen on. Loopback by default; bind a Tailscale address to
    /// accept agents from other machines.
    #[arg(long, env = "FLEETMON_BIND", default_value = "127.0.0.1:7070")]
    bind: SocketAddr,
    /// Comma-separated IPs or CIDRs allowed besides loopback, e.g.
    /// 100.64.0.5,100.64.0.0/10. Applies to agents and the page alike.
    #[arg(long, env = "FLEETMON_ALLOW", value_delimiter = ',', value_parser = parse_net)]
    allow: Vec<IpNet>,
    /// File holding the token agents must present. Falls back to FLEETMON_TOKEN.
    #[arg(long, env = "FLEETMON_TOKEN_FILE")]
    token_file: Option<PathBuf>,
    /// Samples kept per host (at the default 1 s interval, 300 is five minutes).
    #[arg(long, default_value_t = 300, value_parser = clap::value_parser!(u64).range(1..=100_000))]
    history: u64,
}

/// A bare address means just that host.
fn parse_net(s: &str) -> Result<IpNet, String> {
    let s = s.trim();
    s.parse::<IpNet>()
        .or_else(|_| s.parse::<std::net::IpAddr>().map(IpNet::from))
        .map_err(|_| format!("not an IP or CIDR: {s}"))
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
    if token.len() < 16 {
        bail!("token is shorter than 16 characters");
    }

    if !args.bind.ip().is_loopback() && args.allow.is_empty() {
        tracing::warn!(
            "bound to {} with no --allow: only loopback will be served",
            args.bind
        );
    }

    let hub = fleetmon_hub::Hub::new(fleetmon_hub::Config {
        token,
        allow: args.allow,
        history: args.history as usize,
    });
    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .with_context(|| format!("binding {}", args.bind))?;
    tracing::info!("listening on http://{}", args.bind);

    axum::serve(
        listener,
        hub.router()
            .into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
