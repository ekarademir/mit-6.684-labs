mod api;
mod errors;

use std::convert::Infallible;
use std::thread;

use env_logger;
use log::{debug, info, error};
use hyper::{Client, Uri, Server, StatusCode};
use hyper::service::{make_service_fn, service_fn};
use tokio::signal;
use tokio::runtime::Runtime;

fn main() {
    env_logger::init();

    let server_thread = thread::spawn(server_task);
    let client_thread = thread::spawn(wait_for_server_task);

    client_thread.join().unwrap();
    server_thread.join().unwrap();
}

fn server_task() {
    let mut rt = Runtime::new().unwrap();

    rt.block_on(async {
        let make_service = make_service_fn(|_conn| {
            async {
                Ok::<_, Infallible>(
                    service_fn(api::main_service)
                )
            }
        });
        info!("I am a {:?} machine", api::system::kind());
        let addr = ([0, 0, 0, 0], 3000).into();
        info!("Listening on http://{}", addr);

        let server = Server::bind(&addr)
            .serve(make_service)
            .with_graceful_shutdown(async {
                signal::ctrl_c().await.unwrap();
                info!("Shutting down");
            });

        if let Err(e) = server.await {
            error!("Problem bootstrapping the server: {}", e);
        }
    });

}

fn wait_for_server_task() {
    let mut rt = Runtime::new().unwrap();

    rt.block_on(async {
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
    });
}

// TODO tests
