// These tests check what reading a body needs no web framework for: the type
// a body claims to be, its size, and a route that takes no body. How the body
// is read, and the limit of a layer, are part of the tests of the extractors
// for axum.

use http::{Method, Request, StatusCode, request::Parts};
use serde::Deserialize;

use super::{MAX_REQUEST_BODY, body_of, no_body};

#[derive(Debug, Deserialize, PartialEq)]
struct Preview {
    #[serde(rename = "customerId")]
    customer_id: String,
}

fn parts_with(method: Method, headers: &[(&str, &str)]) -> Parts {
    let mut request = Request::builder().method(method).uri("/previews");
    for (name, value) in headers {
        request = request.header(*name, *value);
    }

    request
        .body(())
        .expect("the request is valid")
        .into_parts()
        .0
}

fn json() -> Parts {
    parts_with(Method::POST, &[("content-type", "application/json")])
}

#[test]
fn decodes_a_body_that_fits() {
    for content_type in [
        "application/json",
        "application/json; charset=utf-8",
        "APPLICATION/JSON",
    ] {
        let parts = parts_with(Method::POST, &[("content-type", content_type)]);

        let preview: Preview =
            body_of(&parts, br#"{"customerId":"42"}"#).unwrap_or_else(|failure| {
                panic!("{content_type}: {failure}");
            });

        assert_eq!(preview.customer_id, "42", "{content_type}");
    }
}

#[test]
fn rejects_a_body_that_does_not_claim_to_be_json() {
    for (content_type, message) in [
        (None, "unsupported media type: Content-Type is required"),
        (
            Some("text/plain"),
            r#"unsupported media type: "text/plain" is not application/json"#,
        ),
        (
            Some("text/plain; application/json"),
            r#"unsupported media type: "text/plain" is not application/json"#,
        ),
        (
            Some("not a media type at all;;;"),
            r#"unsupported media type: "not a media type at all;;;" could not be parsed"#,
        ),
    ] {
        let headers: Vec<(&str, &str)> = content_type
            .map(|content_type| ("content-type", content_type))
            .into_iter()
            .collect();
        let parts = parts_with(Method::POST, &headers);

        let failure = body_of::<Preview>(&parts, br#"{"customerId":"42"}"#).expect_err("not JSON");

        assert_eq!(
            failure.status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "{content_type:?}"
        );
        assert_eq!(failure.message(), message, "{content_type:?}");
    }
}

/// A body of the given size that fits.
fn body_of_size(size: usize) -> Vec<u8> {
    let (prefix, suffix) = (r#"{"customerId":""#, r#""}"#);
    let padding = "a".repeat(size - prefix.len() - suffix.len());

    format!("{prefix}{padding}{suffix}").into_bytes()
}

#[test]
fn rejects_a_body_over_the_limit() {
    let failure = body_of::<Preview>(&json(), &body_of_size(MAX_REQUEST_BODY + 1))
        .expect_err("the body is too large");

    assert_eq!(failure.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        failure.message(),
        "request body too large: at most 1048576 bytes are read"
    );
}

#[test]
fn reads_a_body_at_the_limit() {
    let preview: Preview =
        body_of(&json(), &body_of_size(MAX_REQUEST_BODY)).expect("the body is at the limit");

    assert_eq!(preview.customer_id.len(), MAX_REQUEST_BODY - 17);
}

#[test]
fn says_why_a_body_does_not_fit_with_400() {
    let failure = body_of::<Preview>(&json(), br#"{"customerId":42}"#).expect_err("no string");

    assert_eq!(failure.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        failure.message(),
        r#"malformed request: "customerId" must be a string"#
    );
}

#[test]
fn takes_an_empty_body_or_an_empty_object_for_a_route_that_takes_no_body() {
    let parts = parts_with(Method::POST, &[]);

    for body in ["", "{}", " { } ", "{}\n"] {
        assert!(
            no_body(&parts, body.as_bytes()).is_ok(),
            "{body:?} has to be taken"
        );
    }
}

#[test]
fn refuses_any_other_body_for_a_route_that_takes_no_body() {
    let parts = parts_with(Method::POST, &[]);

    for body in [r#"{"quantity":3}"#, "null", "[]", "x"] {
        let failure = no_body(&parts, body.as_bytes()).expect_err(body);

        assert_eq!(failure.status(), StatusCode::BAD_REQUEST, "{body}");
        assert_eq!(
            failure.message(),
            "malformed request: this route takes no body, so the body has to be empty, or {}",
            "{body}"
        );
    }
}

#[test]
fn refuses_a_request_without_a_body_from_another_origin() {
    for (label, headers) in [
        (
            "by its Sec-Fetch-Site",
            vec![("sec-fetch-site", "cross-site")],
        ),
        (
            "by its Sec-Fetch-Site, also from the same site",
            vec![("sec-fetch-site", "same-site")],
        ),
        (
            "by an Origin that is not the host",
            vec![
                ("origin", "https://evil.example"),
                ("host", "library.example"),
            ],
        ),
    ] {
        let failure = no_body(&parts_with(Method::POST, &headers), b"").expect_err(label);

        assert_eq!(failure.status(), StatusCode::FORBIDDEN, "{label}");
        assert_eq!(
            failure.message(),
            "forbidden: a request without a body is not accepted from another origin",
            "{label}"
        );
    }
}

#[test]
fn takes_a_request_without_a_body_from_the_same_origin() {
    for (label, method, headers) in [
        (
            "by its Sec-Fetch-Site",
            Method::POST,
            vec![("sec-fetch-site", "same-origin")],
        ),
        (
            "typed into the address bar",
            Method::POST,
            vec![("sec-fetch-site", "none")],
        ),
        (
            "by an Origin that is the host",
            Method::POST,
            vec![
                ("origin", "https://library.example"),
                ("host", "library.example"),
            ],
        ),
        ("without anything to tell", Method::POST, vec![]),
        (
            "with a safe method from any origin",
            Method::GET,
            vec![("sec-fetch-site", "cross-site")],
        ),
    ] {
        assert!(
            no_body(&parts_with(method, &headers), b"").is_ok(),
            "{label}"
        );
    }
}
