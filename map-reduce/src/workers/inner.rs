use std::net::SocketAddr;
use std::time::Duration;
use std::thread::{self, JoinHandle};

use log::{debug, info, error, warn};
use hyper::{Client, Uri, StatusCode};
use tokio::runtime::Runtime;

use crate::api::{self, system};
use crate::MachineState;

const SLEEP_DURATION_SEC:u64 = 2;
const RETRY_TIMES:usize = 4;

async fn wait_for_server(my_socket: SocketAddr) -> Result<(), ()>{
    let client = Client::new();
    let my_uri = format!("http://{}{}",
        my_socket,
        api::endpoints::HEALTH
    ).parse::<Uri>().unwrap();

    let wait_duration = Duration::from_secs(SLEEP_DURATION_SEC);

    for retry in 1..RETRY_TIMES+1 {
        info!("Probing to server. Trial {} of {}", retry, RETRY_TIMES);
        let result = client.get(my_uri.clone()).await;
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

pub fn spawn_inner(state: MachineState) -> JoinHandle<()> {
    let main_state = state.clone();
    thread::Builder::new().name("Inner".into()).spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async move {
            // Get socket of this machine
            let my_socket = {
                let state = main_state.lock().unwrap();
                state.socket.clone()
            };
            debug!("Waiting for server to come online");
            // Wait until server thread is responding
            match wait_for_server(my_socket).await {
                Ok(_) => info!("Server is online"),
                Err(_) => {
                    error!("Server is offline, panicking");
                    panic!("Server is offline");
                },
            }
            // Update this machine state as ready
            {
                let mut state = main_state.lock().unwrap();
                state.status = system::Status::Ready;
            }
        });
    }).unwrap()
}
