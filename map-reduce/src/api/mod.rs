pub mod whois;

use std::convert::Infallible;

use log::{debug};
use hyper::{Body, Method, Request, Response, StatusCode};
use hyper::header;

pub async fn main_service(request: Request<Body>) -> Result<Response<Body>, Infallible> {
    debug!("Building service");

    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, "/about") => json_response(whois::about(), None),
        _ => json_response(
            String::from("{\"error\": \"Not found\"}"),
            Some(StatusCode::NOT_FOUND)
        )
    };

    Ok(
        response
    )
}

fn json_response(json: String, status: Option<StatusCode>) -> Response<Body> {
    let status = if let Some(code) = status {
        code
    } else {
        StatusCode::OK
    };
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .unwrap()
}
