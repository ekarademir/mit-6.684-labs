use std::thread::{self, JoinHandle};

use log::{debug, info, error};
use hyper::{Client, Uri, StatusCode};
use tokio::runtime::Runtime;

use crate::api::{self, system};
use crate::MachineState;

async fn wait_for_server() {
    let client = Client::new();
    let my_uri = format!("http://0.0.0.0:3000{}",
        api::endpoints::HEALTH).parse::<Uri>()
        .unwrap();
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

pub fn spawn_client(state: MachineState) -> JoinHandle<()> {
    let main_state = state.clone();
    thread::spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async move {
            debug!("Waiting for server to come online");
            wait_for_server().await;
            {
                let mut state = main_state.lock().unwrap();
                state.status = system::Status::Ready;
            }
        });
    })
}
