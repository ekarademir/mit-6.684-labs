use std::fmt::Display;
use std::net::SocketAddr;
use std::time::Duration;
use std::thread::{self, JoinHandle};

use log::{debug, info, error, warn};
use hyper::{Client, Uri, StatusCode};
use tokio::runtime::Runtime;

use crate::api::{self, system};
use crate::MachineState;
use crate::HostPort;

const SLEEP_DURATION_SEC:u64 = 2;
const RETRY_TIMES:usize = 4;

async fn probe_health <T: Display>(addr: T) -> Result<(), ()> {
    let client = Client::new();
    let probe_addr = format!("http://{}{}",
        addr,
        api::endpoints::HEALTH
    ).parse::<Uri>().unwrap();

    let wait_duration = Duration::from_secs(SLEEP_DURATION_SEC);

    for retry in 1..RETRY_TIMES+1 {
        info!("Probing to server {}. Trial {} of {}", probe_addr, retry, RETRY_TIMES);
        let result = client.get(probe_addr.clone()).await;
        match result {
            Ok(response) => {
                match response.status() {
                    StatusCode::OK => {
                        return Ok(());
                    },
                    x => warn!("Server response is {}, will try again.", x),
                }
            },
            _ => warn!("Server did not respond, will try again.")
        }
        thread::sleep(wait_duration);
    }
    error!("Giving up on server wait.");
    Err(())
}

async fn wait_for_server(my_socket: SocketAddr) -> Result<(), ()>{
    probe_health(my_socket).await
}

async fn wait_for_master(master_uri: Uri) -> Result<(), ()>{
    probe_health(master_uri.host_port()).await
}

pub fn spawn_inner(state: MachineState) -> JoinHandle<()> {
    let main_state = state.clone();
    thread::Builder::new().name("Inner".into()).spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async move {
            // Get socket of this machine
            let (
                my_socket,
                my_kind,
                master_uri,
            ) = {
                let state = main_state.lock().unwrap();
                (
                    state.socket.clone(),
                    state.kind.clone(),
                    state.master_uri.clone(),
                )
            };
            debug!("Waiting for server to come online");
            // Wait until server thread is responding
            match wait_for_server(my_socket).await {
                Ok(_) => info!("Server is online"),
                Err(_) => {
                    error!("Server thread is offline, panicking");
                    panic!("Server thread is offline");
                },
            }
            if my_kind == system::MachineKind::Worker {
                if let Some(master) = master_uri {
                    // Wait until master is responding
                    match wait_for_master(master).await {
                        Ok(_) => info!("Master is online"),
                        Err(_) => {
                            error!("Master machine is offline, panicking");
                            panic!("Master machine is offline");
                        },
                    }
                } else {
                    error!("A worker machine has to have a master machine assigned, panicking");
                    panic!("A worker machine has to have a master machine assigned");
                }
            }
            // Update this machine state as ready
            {
                let mut state = main_state.lock().unwrap();
                state.status = system::Status::Ready;
                if my_kind == system::MachineKind::Worker {
                    state.master = Some(
                        system::NetworkNeighbor {
                            addr: state.master_uri.clone().unwrap().to_string(),
                            kind: system::MachineKind::Master,
                            status: system::Status::Online,
                            last_heartbeat_ns: 0,
                        }
                    );
                }
            }
            // Run tasks
            {
                // TODO call assignTask on each worker
            }
        });
    }).unwrap()
}
