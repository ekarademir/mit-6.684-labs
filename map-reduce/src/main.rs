mod server;
mod services;

use std::error::Error;


use env_logger;
use log::{error};


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>>{
    env_logger::init();

    if let Err(e) = server::serve().await {
        error!("Problem bootstrapping the server: {}", e);
    }

    Ok(())
}
