mod server;
mod api;

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
    let addr = vec![
        "https://samples.openweathermap.org/data/2.5/weather?q=London,uk&appid=439d4b804bc8187953eb36d2a8c26a02",
        "https://samples.openweathermap.org/data/2.5/weather?q=London,uk&appid=439d4b804bc8187953eb36d2a8c26a02",
    ];

    let mut f = FuturesUnordered::new();

    for a in addr.iter() {
        f.push(fetch_url(Uri::from_static(a)))
    }

    while let Some(status_result) = f.next().await {
        match status_result {
            Ok(status) => println!("{}", status),
            Err(e) => println!("{:?}", e)
        }
    }

    Ok(())
}

async fn fetch_url(url: Uri) -> Result<StatusCode, hyper::error::Error>{
    let client = Client::new();
    client.get(url).await.map(|r| r.status())
}

// Move server to own module folder
// Add a subfolder for jobs
