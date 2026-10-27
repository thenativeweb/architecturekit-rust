//! A local HTTP server that stands in for EventSourcingDB and answers one route
//! as a test tells it, as the refusing database of ArchitectureKit for Go does.

use eventsourcingdb::Client;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};
use url::Url;

/// Which API the fake offers. A client asks for the v2 API first, and falls
/// back to the v1 API if the server answers that with 404.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum Api {
    V1,
    V2,
}

/// How the fake answers the route of a test.
#[derive(Clone, Debug)]
pub(super) struct Reply {
    pub(super) status: u16,
    pub(super) body: String,
    pub(super) is_from_eventsourcingdb: bool,
}

impl Reply {
    /// An answer with the given status and text, as EventSourcingDB sends it.
    pub(super) fn status(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.to_owned(),
            is_from_eventsourcingdb: true,
        }
    }

    /// An answer with the given status from a server that is not an
    /// EventSourcingDB, such as a proxy in front of it.
    pub(super) fn foreign(status: u16) -> Self {
        Self {
            status,
            body: "bad gateway".to_owned(),
            is_from_eventsourcingdb: false,
        }
    }

    /// An error with a code, as the v2 API sends it with status 200.
    pub(super) fn error_with_code(code: &str, description: &str) -> Self {
        let item = serde_json::json!({
            "type": "error",
            "data": { "code": code, "description": description, "details": {} },
        });

        Self {
            status: 200,
            body: format!("{item}\n"),
            is_from_eventsourcingdb: true,
        }
    }
}

/// Starts a fake that offers the given API and answers requests to the given
/// path as the reply says, and every other route with 404, and returns a
/// client of it.
pub(super) async fn refusing(api: Api, path: &'static str, reply: Reply) -> Client {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the fake binds");
    let address = listener.local_addr().expect("the fake has an address");

    drop(tokio::spawn(async move {
        while let Ok((connection, _)) = listener.accept().await {
            let reply = reply.clone();
            drop(tokio::spawn(async move {
                answer(connection, api, path, &reply).await;
            }));
        }
    }));

    let base_url: Url = format!("http://{address}")
        .parse()
        .expect("the address is a URL");
    Client::new(base_url, "secret")
}

async fn answer(connection: TcpStream, api: Api, path: &str, reply: &Reply) {
    let mut reader = BufReader::new(connection);

    let mut request_line = String::new();
    if reader.read_line(&mut request_line).await.unwrap_or(0) == 0 {
        return;
    }
    let requested = request_line
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_owned();

    let mut content_length = 0;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).await.unwrap_or(0) == 0 || header == "\r\n" {
            break;
        }
        if let Some((name, value)) = header.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            content_length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0; content_length];
    let _ = reader.read_exact(&mut body).await;

    let response = if requested == "/api/v2/system/ping" {
        match api {
            Api::V2 => Reply::status(200, "{\"type\":\"event\",\"data\":{}}\n"),
            Api::V1 => Reply::status(404, "not found"),
        }
    } else if requested == path {
        reply.clone()
    } else {
        Reply::status(404, "not found")
    };

    let server = if response.is_from_eventsourcingdb {
        "Server: EventSourcingDB/test\r\n"
    } else {
        "Server: proxy\r\n"
    };
    let head = format!(
        "HTTP/1.1 {} Fake\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n{server}Connection: close\r\n\r\n",
        response.status,
        response.body.len(),
    );

    let mut connection = reader.into_inner();
    let _ = connection.write_all(head.as_bytes()).await;
    let _ = connection.write_all(response.body.as_bytes()).await;
    let _ = connection.shutdown().await;
}
