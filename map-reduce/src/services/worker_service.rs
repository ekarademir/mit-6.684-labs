
use std::convert::Infallible;

use log::{debug};
use hyper::{Body, Request, Response};

pub async fn worker_service(_request: Request<Body>) -> Result<Response<Body>, Infallible> {
    debug!("Building worker service");
    Ok(
        Response::new(
            Body::from(
                "I am worker"
            )
        )
    )
}
