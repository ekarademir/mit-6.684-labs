use std::thread::{self, JoinHandle};

use log::{debug, info};
use hyper::{Client, Uri, StatusCode};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, watch};

use crate::api::{self, system};

async fn wait_for_server() {
    let client = Client::new();
    let my_uri = format!("http://0.0.0.0:3000{}",
        api::endpoints::HEALTH).parse::<Uri>()
        .unwrap();
    debug!("Waiting for server to come online");
    while let Ok(response) = client.get(my_uri.clone()).await {
        if response.status() == StatusCode::OK {
            info!("Server is online");
            break;
        }
    }
}

pub fn spawn_client(
    state_receiver: watch::Receiver<system::Status>,
    worker_sender: mpsc::Sender<system::Status>
) -> JoinHandle<()> {
    thread::spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async {
            wait_for_server().await;
        });
    })
}
