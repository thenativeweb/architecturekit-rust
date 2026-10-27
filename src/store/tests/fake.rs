//! A local HTTP server that stands in for EventSourcingDB, as the fake
//! databases of ArchitectureKit for Go do. It answers the routes a test gives
//! it as the test tells it, and every other route with an empty result, as for
//! a subject without events. It records the path of every request, and handles
//! one connection after the other, which is enough for the requests of a test.

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use eventsourcingdb::Client;
use serde_json::Value;
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

/// An answer of the fake.
#[derive(Clone, Debug)]
pub(super) struct Reply {
    pub(super) status: u16,
    pub(super) body: String,
    pub(super) headers: Vec<(String, String)>,
    pub(super) is_from_eventsourcingdb: bool,
}

impl Reply {
    /// An answer with the given status and text, as EventSourcingDB sends it.
    pub(super) fn status(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.to_owned(),
            headers: Vec::new(),
            is_from_eventsourcingdb: true,
        }
    }

    /// An answer with the given status from a server that is not an
    /// EventSourcingDB, such as a proxy in front of it.
    pub(super) fn foreign(status: u16) -> Self {
        Self {
            is_from_eventsourcingdb: false,
            ..Self::status(status, "bad gateway")
        }
    }

    /// A redirect of a proxy in front of the database to the given path.
    pub(super) fn redirect(status: u16, location: &str) -> Self {
        Self {
            headers: vec![("Location".to_owned(), location.to_owned())],
            ..Self::foreign(status)
        }
    }

    /// An error with a code, as the v2 API sends it with status 200.
    pub(super) fn error_with_code(code: &str, description: &str) -> Self {
        Self::error_with_details(code, description, &serde_json::json!({}))
    }

    /// An error with a code and details, as the v2 API sends it with status
    /// 200.
    pub(super) fn error_with_details(code: &str, description: &str, details: &Value) -> Self {
        let item = serde_json::json!({
            "type": "error",
            "data": { "code": code, "description": description, "details": details },
        });

        Self::status(200, &format!("{item}\n"))
    }
}

/// What the fake does with a request to one of the routes of a test.
#[derive(Clone, Debug)]
pub(super) enum Answer {
    /// Answers with the reply, once it has read the request completely.
    Reply(Reply),
    /// Answers a POST with the reply, and any other method with 405.
    ToPost(Reply),
    /// Hangs up without answering, once it has read the request completely,
    /// as a database does that goes down while it writes.
    HangUp,
    /// Hangs up as soon as it has the headers of the request, without reading
    /// its body, as a database does that goes down while the request arrives.
    HangUpOnHeaders,
    /// Announces the reply completely, but sends only the first half of it
    /// before it hangs up.
    CutOff(Reply),
    /// Answers nothing for the given time, and then hangs up.
    Stall(Duration),
}

/// A fake that is about to start.
#[derive(Debug)]
pub(super) struct Fake {
    api: Api,
    routes: Vec<(&'static str, Answer)>,
    vanishes_after: Option<&'static str>,
}

impl Fake {
    /// A fake that offers the given API.
    pub(super) fn new(api: Api) -> Self {
        Self {
            api,
            routes: Vec::new(),
            vanishes_after: None,
        }
    }

    /// Answers requests to the given path as the answer says.
    pub(super) fn route(mut self, path: &'static str, answer: Answer) -> Self {
        self.routes.push((path, answer));
        self
    }

    /// Stops taking connections once it has answered a request to the given
    /// path, so that a request after it can not even connect, as with a
    /// database that has gone down in between.
    pub(super) fn vanishing_after(mut self, path: &'static str) -> Self {
        self.vanishes_after = Some(path);
        self
    }

    /// Starts the fake, and returns its URL and the requests it receives.
    pub(super) async fn start(self) -> (Url, Requests) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("the fake binds");
        let address = listener.local_addr().expect("the fake has an address");
        let requests = Requests::default();

        let recorded = requests.clone();
        drop(tokio::spawn(async move {
            while let Ok((connection, _)) = listener.accept().await {
                let path = self.answer(connection, &recorded).await;
                if self
                    .vanishes_after
                    .is_some_and(|vanishing| path.as_deref() == Some(vanishing))
                {
                    break;
                }
            }
        }));

        let url = format!("http://{address}")
            .parse()
            .expect("the address is a URL");
        (url, requests)
    }

    /// Starts the fake, and returns a client of it.
    pub(super) async fn client(self) -> Client {
        Client::new(self.start().await.0, "secret")
    }

    /// Answers a request, and returns its path.
    async fn answer(&self, connection: TcpStream, requests: &Requests) -> Option<String> {
        let mut reader = BufReader::new(connection);

        let mut request_line = String::new();
        if reader.read_line(&mut request_line).await.unwrap_or(0) == 0 {
            return None;
        }
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or_default().to_owned();
        let path = parts.next().unwrap_or_default().to_owned();
        requests.record(&path);

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

        let answer = self.answer_to(&path);
        if matches!(answer, Answer::HangUpOnHeaders) {
            return Some(path);
        }

        let mut body = vec![0; content_length];
        let _ = reader.read_exact(&mut body).await;
        let mut connection = reader.into_inner();

        let reply = match answer {
            Answer::Reply(reply) => reply,
            Answer::ToPost(reply) if method == "POST" => reply,
            Answer::ToPost(_) => Reply::status(405, "method not allowed"),
            Answer::HangUp | Answer::HangUpOnHeaders => return Some(path),
            Answer::CutOff(reply) => {
                let half = &reply.body.as_bytes()[..reply.body.len() / 2];
                let _ = connection.write_all(head_of(&reply).as_bytes()).await;
                let _ = connection.write_all(half).await;
                return Some(path);
            }
            Answer::Stall(duration) => {
                tokio::time::sleep(duration).await;
                return Some(path);
            }
        };

        let _ = connection.write_all(head_of(&reply).as_bytes()).await;
        let _ = connection.write_all(reply.body.as_bytes()).await;
        let _ = connection.shutdown().await;

        Some(path)
    }

    /// Returns what to do with a request to the given path: what the test
    /// says, or the empty result of the API, or, for the ping with which a
    /// client asks for the API, its answer.
    fn answer_to(&self, path: &str) -> Answer {
        if path == "/api/v2/system/ping" {
            return Answer::Reply(match self.api {
                Api::V2 => Reply::status(200, "{\"type\":\"event\",\"data\":{}}\n"),
                Api::V1 => Reply::status(404, "not found"),
            });
        }

        if let Some((_, answer)) = self.routes.iter().find(|(route, _)| *route == path) {
            return answer.clone();
        }

        Answer::Reply(match self.api {
            Api::V2 => Reply::status(200, "{\"type\":\"end\",\"data\":{}}\n"),
            Api::V1 => Reply::status(200, ""),
        })
    }
}

/// The status line and headers of a reply.
fn head_of(reply: &Reply) -> String {
    let server = if reply.is_from_eventsourcingdb {
        "Server: EventSourcingDB/test\r\n"
    } else {
        "Server: proxy\r\n"
    };
    let mut head = format!(
        "HTTP/1.1 {} Fake\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n{server}",
        reply.status,
        reply.body.len(),
    );
    for (name, value) in &reply.headers {
        for part in [name, ": ", value, "\r\n"] {
            head.push_str(part);
        }
    }
    head.push_str("Connection: close\r\n\r\n");

    head
}

/// The paths of the requests a fake has received, in their order.
#[derive(Clone, Debug, Default)]
pub(super) struct Requests(Arc<Mutex<Vec<String>>>);

impl Requests {
    fn record(&self, path: &str) {
        self.0
            .lock()
            .expect("no test panics while recording")
            .push(path.to_owned());
    }

    /// Returns how many requests the fake has received, the ping with which a
    /// client asks for the API included.
    pub(super) fn count(&self) -> usize {
        self.0.lock().expect("no test panics while recording").len()
    }

    /// Returns how many requests to the given path the fake has received.
    pub(super) fn to(&self, path: &str) -> usize {
        self.0
            .lock()
            .expect("no test panics while recording")
            .iter()
            .filter(|recorded| *recorded == path)
            .count()
    }
}

/// Returns a client of an address that nothing listens on, as of a database
/// that can not be reached.
pub(super) async fn dead_client() -> Client {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the address binds");
    let address = listener.local_addr().expect("the address is known");
    drop(listener);

    let url = format!("http://{address}")
        .parse()
        .expect("the address is a URL");
    Client::new(url, "secret")
}

/// Starts a fake that offers the given API and answers requests to the given
/// path as the reply says, and returns a client of it.
pub(super) async fn refusing(api: Api, path: &'static str, reply: Reply) -> Client {
    Fake::new(api)
        .route(path, Answer::Reply(reply))
        .client()
        .await
}
