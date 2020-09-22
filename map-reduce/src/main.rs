mod api;
mod errors;

use std::error::Error;
use std::convert::Infallible;

use env_logger;
use log::{info, error};
use hyper::{Server};
use hyper::service::{make_service_fn, service_fn};

#[tokio::main]
async fn main() {
    env_logger::init();

    let make_service = make_service_fn(|_conn| {
        async {
            Ok::<_, Infallible>(
                service_fn(api::main_service)
            )
        }
    });


    info!("I am a {:?} machine", api::system::kind());
    let addr = ([0, 0, 0, 0], 3000).into();
    info!("Listening on http://{}", addr);

    let server = Server::bind(&addr).serve(make_service);


    if let Err(e) = server.await {
        error!("Problem bootstrapping the server: {}", e);
    }
}


// TODO tests
