mod server;
mod api;
mod errors;

use std::error::Error;


use env_logger;
use log::{info, error};
use futures::join;
use futures::stream::{self, StreamExt};
use futures::stream::FuturesUnordered;

use hyper::{Client, Uri, StatusCode};


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>>{
    env_logger::init();

    info!("I am a {:?} machine", server::kind());

    if let Err(e) = server::serve().await {
        error!("Problem bootstrapping the server: {}", e);
    }
    Ok(())
}
