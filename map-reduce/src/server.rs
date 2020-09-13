use super::api::main_service;

use std::convert::Infallible;
use std::error::Error;

use log::{info, error};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Server};


pub async fn serve() -> Result<(), Box<dyn Error + Send + Sync>> {
    let make_worker_service = make_service_fn(|_conn| {
        async {
            Ok::<_, Infallible>(
                service_fn(main_service)
            )
        }
    });
    let addr = ([0, 0, 0, 0], 3000).into();
    let server = Server::bind(&addr).serve(make_worker_service);

    info!("Listening on http://{}", addr);

    if let Err(e) = server.await {
        error!("server error: {}", e);
    }

    Ok(())
}

pub use super::api::kind::server_kind;
