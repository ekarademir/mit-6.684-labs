mod server;
mod api;

use std::error::Error;


use env_logger;
use log::{info, error};


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>>{
    env_logger::init();

    info!("I am a {:?} machine", server::kind());

    if let Err(e) = server::serve().await {
        error!("Problem bootstrapping the server: {}", e);
    }

    Ok(())
}

// Move server to own module folder
// Add a subfolder for jobs
