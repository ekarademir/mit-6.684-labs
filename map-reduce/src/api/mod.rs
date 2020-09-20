pub mod system;
mod endpoints;

use std::convert::Infallible;

use log::{debug};
use hyper::{Body, Method, Request, Response, StatusCode};
use hyper::header;

pub async fn main_service(request: Request<Body>) -> Result<Response<Body>, Infallible> {
    debug!("Building service");

    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, endpoints::ABOUT) => json_response(system::about().await, None),
        (&Method::GET, endpoints::HEALTH) => json_response(system::health().await, None),
        _ => not_found()
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

fn not_found() -> Response<Body> {
    json_response(
        String::from("{\"error\": \"Not found\"}"),  // TODO: Properly serialize this
        Some(StatusCode::NOT_FOUND)
    )
}
