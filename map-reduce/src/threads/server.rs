use std::thread::{self, JoinHandle};

use log::{info, error, warn};
use hyper::Server;
use tokio::signal;
use tokio::runtime::Runtime;
use tokio::select;
use tokio::sync::{mpsc, oneshot};

use crate::api;
use crate::system;
use crate::MachineState;

pub fn spawn_server(
    state: MachineState,
    heartbeat_sender: mpsc::Sender<system::NetworkNeighbor>,
    kill_rx: oneshot::Receiver<()>
) -> JoinHandle<()> {
    let main_state = state.clone();
    thread::Builder::new().name("Server".into()).spawn(|| {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (addr, kind) = {
                let state = main_state.lock().unwrap();
                (state.socket, state.kind)
            };
            info!("I am a {:?} machine", kind);
            info!("Listening on http://{}", addr);

            let bound_server = Server::try_bind(&addr);

            if let Ok(server) = bound_server {
                let server = server
                    .serve(api::MakeMainService {
                        state: main_state,
                        heartbeat_sender,
                    })
                    .with_graceful_shutdown(async {
                        select! {
                            _ = kill_rx => {
                                warn!("Received shutdown signal from main thread ...");
                            }
                            _ = signal::ctrl_c() => {
                                warn!("Received Ctrl+C ...");
                            }
                        }
                        warn!("... gracefully shutting down");
                    });

                if let Err(e) = server.await {
                    error!("Problem starting the server: {}", e);
                }
            } else {
                error!("Can't bind server to given TCP socket, panicking");
                panic!("Can't bind server to given TCP socket");
            }

        });
    }).unwrap()
}
