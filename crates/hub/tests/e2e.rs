//! A real agent against a real hub, observed the way the page observes it.

use std::{net::SocketAddr, time::Duration};

use fleetmon_hub::{Config, Hub};
use fleetmon_proto::UiMsg;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

const TOKEN: &str = "test-token-0123456789";

async fn start_hub() -> SocketAddr {
    let hub = Hub::new(Config {
        token: TOKEN.into(),
        allow: vec![],
        history: 10,
        max_hosts: 8,
        store: None,
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            hub.router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    addr
}

type Reader =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn next_ui(ws: &mut Reader) -> UiMsg {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("hub went quiet")
            .unwrap()
            .unwrap();
        if let Message::Text(t) = frame {
            return serde_json::from_str(&t).unwrap();
        }
    }
}

#[tokio::test]
async fn agent_shows_up_streams_and_goes_offline() {
    let addr = start_hub().await;
    let (mut ui, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/ws"))
        .await
        .unwrap();
    assert_eq!(next_ui(&mut ui).await, UiMsg::Snapshot { hosts: vec![] });

    let agent = tokio::spawn(fleetmon_agent::run(fleetmon_agent::Config {
        hub: format!("ws://{addr}"),
        token: TOKEN.into(),
        name: "e2e".into(),
        interval: Duration::from_millis(250),
        top: 3,
    }));

    match next_ui(&mut ui).await {
        UiMsg::Host { info } => assert_eq!(info.name, "e2e"),
        other => panic!("expected host, got {other:?}"),
    }
    match next_ui(&mut ui).await {
        UiMsg::Sample { host, sample } => {
            assert_eq!(host, "e2e");
            assert!(sample.mem_used > 0);
        }
        other => panic!("expected sample, got {other:?}"),
    }

    let body = get(addr, "/api/hosts").await;
    assert!(body.contains(r#""name":"e2e""#), "{body}");
    assert!(get(addr, "/").await.contains("<title>fleetmon</title>"));

    agent.abort();
    loop {
        match next_ui(&mut ui).await {
            UiMsg::Offline { host } => break assert_eq!(host, "e2e"),
            UiMsg::Sample { .. } => continue, // one may have been in flight
            other => panic!("expected offline, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn wrong_token_is_refused() {
    let addr = start_hub().await;
    for auth in [Some("Bearer nope"), None] {
        let mut req = format!("ws://{addr}/agent").into_client_request().unwrap();
        if let Some(a) = auth {
            req.headers_mut()
                .insert("authorization", a.parse().unwrap());
        }
        let err = tokio_tungstenite::connect_async(req)
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("401"), "{err}");
    }
}

#[tokio::test]
async fn refused_hello_says_why() {
    let addr = start_hub().await;
    let mut req = format!("ws://{addr}/agent").into_client_request().unwrap();
    req.headers_mut()
        .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await.unwrap();
    let hello = serde_json::json!({
        "type": "hello", "name": "", "os": "", "cpu_model": "",
        "cores": 1, "mem_total": 1, "agent_version": "0",
    });
    ws.send(Message::text(hello.to_string())).await.unwrap();
    match ws.next().await.unwrap().unwrap() {
        Message::Close(Some(f)) => {
            assert_eq!(u16::from(f.code), fleetmon_proto::CLOSE_REFUSED);
            assert!(f.reason.contains("empty"), "{}", f.reason);
        }
        other => panic!("expected a close frame, got {other:?}"),
    }
}

async fn start_hub_with_db(db: &std::path::Path) -> SocketAddr {
    let hub = Hub::new(Config {
        token: TOKEN.into(),
        allow: vec![],
        history: 300,
        max_hosts: 8,
        store: Some(fleetmon_hub::store::Store::open(db).unwrap()),
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            hub.router()
                .into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    addr
}

fn agent(addr: SocketAddr) -> tokio::task::JoinHandle<anyhow::Result<()>> {
    tokio::spawn(fleetmon_agent::run(fleetmon_agent::Config {
        hub: format!("ws://{addr}"),
        token: TOKEN.into(),
        name: "hist".into(),
        interval: Duration::from_millis(250),
        top: 3,
    }))
}

#[tokio::test]
async fn history_is_stored_served_and_survives_a_hub_restart() {
    let db = std::env::temp_dir().join(format!("fleetmon-e2e-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&db);

    let addr = start_hub_with_db(&db).await;
    let a = agent(addr);
    // Samples every 250 ms; the store's writer flushes about once a second.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    a.abort();

    let hour = get(addr, "/api/history?host=hist&range=1h").await;
    assert!(hour.contains("\"cpu_pct\""), "{hour}");
    let live = get(addr, "/api/history?host=hist&range=5m").await;
    assert!(live.contains("\"cpu_pct\""), "{live}");
    assert!(
        get_status(addr, "/api/history?host=hist&range=2y")
            .await
            .starts_with("HTTP/1.1 400")
    );

    // A new hub on the same file: the reconnecting host's live window is
    // reloaded, so the page's charts continue across a restart.
    let addr2 = start_hub_with_db(&db).await;
    let (mut ui, _) = tokio_tungstenite::connect_async(format!("ws://{addr2}/ws"))
        .await
        .unwrap();
    assert_eq!(next_ui(&mut ui).await, UiMsg::Snapshot { hosts: vec![] });
    let a2 = agent(addr2);
    let restored = loop {
        if let UiMsg::Snapshot { hosts } = next_ui(&mut ui).await {
            break hosts[0].history.len();
        }
    };
    a2.abort();
    assert!(restored >= 3, "only {restored} samples came back");
    let _ = std::fs::remove_file(&db);
}

async fn get_status(addr: SocketAddr, path: &str) -> String {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(
        format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    out
}

/// Minimal HTTP GET, to avoid a client dependency for two assertions.
async fn get(addr: SocketAddr, path: &str) -> String {
    let mut s = tokio::net::TcpStream::connect(addr).await.unwrap();
    s.write_all(format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).await.unwrap();
    assert!(
        out.starts_with("HTTP/1.0 200") || out.starts_with("HTTP/1.1 200"),
        "{out}"
    );
    out
}
