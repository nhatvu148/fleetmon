//! A real agent against a real hub, observed the way the page observes it.

use std::{net::SocketAddr, time::Duration};

use fleetmon_hub::{Config, Hub};
use fleetmon_proto::UiMsg;
use futures_util::StreamExt;
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
