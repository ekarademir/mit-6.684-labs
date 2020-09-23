mod api;
mod errors;

use std::convert::Infallible;
use std::thread;

use env_logger;
use log::{info, error};
use hyper::{Server};
use hyper::service::{make_service_fn, service_fn};
use tokio::signal;
use tokio::runtime::Runtime;

fn main() {
    env_logger::init();

    let server_handler = thread::spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async {
            // Make server and kick off
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

            let server = Server::bind(&addr)
                .serve(make_service)
                .with_graceful_shutdown(async {
                    signal::ctrl_c().await.unwrap();
                    info!("Shutting down");
                });

            if let Err(e) = server.await {
                error!("Problem bootstrapping the server: {}", e);
            }
        });

    });

    info!("OSMAN");

    server_handler.join().unwrap();
}

// TODO tests
