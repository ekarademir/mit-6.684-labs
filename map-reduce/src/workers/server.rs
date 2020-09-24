use std::convert::Infallible;

use log::{info, error};
use hyper::Server;
use hyper::service::{make_service_fn, service_fn};
use tokio::signal;
use tokio::runtime::Runtime;

use crate::api::{self, system};

pub fn server_worker() -> fn() {
    fn inner() {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async {
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
    }

    inner
}
