use std::net::SocketAddr;
use std::thread::{self, JoinHandle};

use log::{debug, info, error};
use hyper::{Client, Uri, StatusCode};
use tokio::runtime::Runtime;

use crate::api::{self, system};
use crate::MachineState;

async fn wait_for_server(my_socket: SocketAddr) {
    let client = Client::new();
    let my_uri = format!("http://{}{}",
        my_socket,
        api::endpoints::HEALTH
    ).parse::<Uri>().unwrap();
    while let Ok(response) = client.get(my_uri.clone()).await {
        if response.status() == StatusCode::OK {
            info!("Server is online");
            break;
        } else if response.status() == StatusCode::NOT_FOUND {
            error!("Server can't respond to health queries");
            break;
        }
    }
}

async fn check_network() {

}

pub fn spawn_init(state: MachineState) -> JoinHandle<()> {
    let main_state = state.clone();
    thread::spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async move {
            // Get socket of this machine
            let my_socket = {
                let state = main_state.lock().unwrap();
                state.socket.clone()
            };
            debug!("Waiting for server to come online");
            // Wait until server thread is responding
            wait_for_server(my_socket).await;
            // Update this machine state as ready
            {
                let mut state = main_state.lock().unwrap();
                state.status = system::Status::Ready;
            }
            // Get network status
        });
    })
}
