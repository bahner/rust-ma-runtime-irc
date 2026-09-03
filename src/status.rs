use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::root::{RoomGraphSnapshot, RootActor};

#[derive(Debug, Clone)]
pub struct StatusConfig {
    pub bind: String,
    pub did: String,
    pub slug: String,
}

#[derive(Debug, Clone)]
pub struct RuntimeState {
    pub root: Arc<Mutex<RootActor>>,
}

#[derive(Debug, Serialize)]
struct StatusJson<'a> {
    did: &'a str,
    slug: &'a str,
    status_bind: &'a str,
    service: &'a str,
}

#[derive(Debug, Deserialize)]
struct ConcourseRequest {
    from: String,
    command: String,
}

#[derive(Debug, Serialize)]
struct ConcourseResponse {
    ok: bool,
    output: String,
}

#[derive(Debug, Serialize)]
struct ConcourseStateResponse {
    ok: bool,
    did: String,
    slug: String,
    graph: RoomGraphSnapshot,
}

pub async fn run_status_server(cfg: StatusConfig, state: RuntimeState) -> Result<()> {
    let listener = TcpListener::bind(&cfg.bind)
        .await
        .with_context(|| format!("binding status server on {}", cfg.bind))?;
    println!("status server listening on http://{}/status.json", cfg.bind);
    println!(
        "concourse command endpoint: POST http://{}/concourse/cmd",
        cfg.bind
    );
    println!(
        "concourse state endpoint: GET http://{}/concourse/state",
        cfg.bind
    );

    loop {
        let (mut socket, _peer) = listener.accept().await.context("accept status socket")?;

        if let Err(error) = handle_connection(&cfg, &state, &mut socket).await {
            eprintln!("status request error: {error}");
        }
    }
}

async fn handle_connection(
    cfg: &StatusConfig,
    state: &RuntimeState,
    socket: &mut tokio::net::TcpStream,
) -> Result<()> {
    let mut buf = [0u8; 8192];
    let read = socket.read(&mut buf).await.context("read status request")?;
    if read == 0 {
        return Ok(());
    }

    let req = String::from_utf8_lossy(&buf[..read]);
    let first = req.lines().next().unwrap_or_default();
    let mut first_parts = first.split_whitespace();
    let method = first_parts.next().unwrap_or("GET");
    let path = first_parts.next().unwrap_or("/");

    if method == "OPTIONS" {
        write_response(socket, "204 No Content", "text/plain; charset=utf-8", b"").await?;
        return Ok(());
    }

    if method == "GET" && path == "/status.json" {
        let body = serde_json::to_string(&StatusJson {
            did: &cfg.did,
            slug: &cfg.slug,
            status_bind: &cfg.bind,
            service: "ma-irc",
        })
        .context("serialising status JSON")?;
        write_response(socket, "200 OK", "application/json", body.as_bytes()).await?;
        return Ok(());
    }

    if method == "GET" && path == "/concourse/help" {
        let output = {
            let root = state.root.lock().await;
            root.concourse_instructions()
        };
        let body = serde_json::to_string(&ConcourseResponse { ok: true, output })
            .context("serialising concourse help JSON")?;
        write_response(socket, "200 OK", "application/json", body.as_bytes()).await?;
        return Ok(());
    }

    if method == "GET" && path == "/concourse/state" {
        let graph = {
            let root = state.root.lock().await;
            root.room_graph_snapshot()
        };
        let body = serde_json::to_string(&ConcourseStateResponse {
            ok: true,
            did: cfg.did.clone(),
            slug: cfg.slug.clone(),
            graph,
        })
        .context("serialising concourse state JSON")?;
        write_response(socket, "200 OK", "application/json", body.as_bytes()).await?;
        return Ok(());
    }

    if method == "POST" && path == "/concourse/cmd" {
        let body_raw = req.split("\r\n\r\n").nth(1).unwrap_or_default().trim();
        if body_raw.is_empty() {
            let body = serde_json::to_string(&ConcourseResponse {
                ok: false,
                output: "missing JSON body".to_string(),
            })
            .context("serialising empty-body response")?;
            write_response(
                socket,
                "400 Bad Request",
                "application/json",
                body.as_bytes(),
            )
            .await?;
            return Ok(());
        }

        let request: ConcourseRequest = match serde_json::from_str(body_raw) {
            Ok(value) => value,
            Err(error) => {
                let body = serde_json::to_string(&ConcourseResponse {
                    ok: false,
                    output: format!("invalid JSON: {error}"),
                })
                .context("serialising invalid-json response")?;
                write_response(
                    socket,
                    "400 Bad Request",
                    "application/json",
                    body.as_bytes(),
                )
                .await?;
                return Ok(());
            }
        };

        let result = {
            let mut root = state.root.lock().await;
            root.concourse_command(&request.from, &request.command)
        };

        let (status, response) = match result {
            Ok(output) => ("200 OK", ConcourseResponse { ok: true, output }),
            Err(error) => (
                "400 Bad Request",
                ConcourseResponse {
                    ok: false,
                    output: error.to_string(),
                },
            ),
        };

        let body = serde_json::to_string(&response)
            .context("serialising concourse command JSON response")?;
        write_response(socket, status, "application/json", body.as_bytes()).await?;
        return Ok(());
    }

    let body = b"ma-irc status endpoint\n";
    write_response(socket, "200 OK", "text/plain; charset=utf-8", body).await?;
    Ok(())
}

async fn write_response(
    socket: &mut tokio::net::TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> Result<()> {
    let header = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nAccess-Control-Allow-Origin: *\r\nAccess-Control-Allow-Methods: GET, POST, OPTIONS\r\nAccess-Control-Allow-Headers: Content-Type\r\nConnection: close\r\n\r\n",
        body.len()
    );
    socket
        .write_all(header.as_bytes())
        .await
        .context("write status header")?;
    socket.write_all(body).await.context("write status body")?;
    Ok(())
}
