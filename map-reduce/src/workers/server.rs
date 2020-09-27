use std::thread::{self, JoinHandle};

use log::{info, error};
use hyper::Server;
use tokio::signal;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, watch};

use crate::api::{self, system};

pub fn spawn_server() -> JoinHandle<()> {
    thread::spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async {
            info!("I am a {:?} machine", api::system::kind());
            let addr = ([0, 0, 0, 0], 3000).into();
            info!("Listening on http://{}", addr);

            let server = Server::bind(&addr)
                .serve(api::MakeMainService {})
                .with_graceful_shutdown(async {
                    signal::ctrl_c().await.unwrap();
                    info!("Shutting down");
                });

            if let Err(e) = server.await {
                error!("Problem bootstrapping the server: {}", e);
            }
        });
    })
}
