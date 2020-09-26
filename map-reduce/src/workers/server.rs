use std::convert::Infallible;
use std::thread::{self, JoinHandle};

use log::{info, error};
use hyper::Server;
use hyper::service::{make_service_fn, service_fn};
use tokio::signal;
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, watch};

use crate::api::{self, system};

pub fn spawn_server(
    state_receiver: watch::Receiver<system::Status>,
    worker_sender: mpsc::Sender<system::Status>
) -> JoinHandle<()> {
    thread::spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async {
            // let make_service = make_service_fn(|_conn| {
            //     async {
            //         Ok::<_, Infallible>(
            //             service_fn(api::main_service)
            //         )
            //     }
            // });
            info!("I am a {:?} machine", api::system::kind());
            let addr = ([0, 0, 0, 0], 3000).into();
            info!("Listening on http://{}", addr);

            let server = Server::bind(&addr)
                // .serve(make_service)
                .serve(api::MainService {
                    status_receiver: state_receiver,
                    status_sender: worker_sender,
                })
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
// Bunu sunun gibi yapacagiz
// https://github.com/hyperium/hyper/blob/master/examples/service_struct_impl.rs
// https://docs.rs/hyper/0.13.8/hyper/service/trait.Service.html

// Refactor server API to a struct
