use std::thread::{self, JoinHandle};

use log::{info, error};
use hyper::Server;
use tokio::signal;
use tokio::runtime::Runtime;

use crate::api;
use crate::MachineState;

pub fn spawn_server(state: MachineState) -> JoinHandle<()> {
    let main_state = state.clone();
    thread::spawn(|| {
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
                        state: main_state
                    })
                    .with_graceful_shutdown(async {
                        signal::ctrl_c().await.unwrap();
                        info!("Shutting down");
                    });

                if let Err(e) = server.await {
                    error!("Problem starting the server: {}", e);
                }
            } else {
                error!("Can't bind server to given TCP socket");
                panic!("Can't bind server to given TCP socket");
            }

        });
    })
}
