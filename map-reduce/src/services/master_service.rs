
use std::convert::Infallible;

use log::{debug};
use hyper::{Body, Request, Response};

pub async fn master_service(_request: Request<Body>) -> Result<Response<Body>, Infallible> {
    debug!("Building master service");
    Ok(
        Response::new(
            Body::from(
                "I am master"
            )
        )
    )
}
