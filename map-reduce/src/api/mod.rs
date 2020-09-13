pub mod kind;

use std::convert::Infallible;

use log::{debug};
use hyper::{Body, Request, Response};

pub async fn main_service(_request: Request<Body>) -> Result<Response<Body>, Infallible> {
    debug!("Building worker service");
    Ok(
        kind::kind()
    )
}
