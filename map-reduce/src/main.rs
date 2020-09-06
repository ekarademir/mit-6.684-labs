use std::convert::Infallible;
use std::error::Error;

use env_logger;
use log::{debug, info, error};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

async fn hello_world(_: Request<Body>) -> Result<Response<Body>, Infallible> {
    debug!("Responding to hello_world request");
    Ok(
        Response::new(
            Body::from(
                "Hello World!"
            )
        )
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>>{
    env_logger::init();

    let make_hello_world_svc = make_service_fn(|_conn| {
        async {
            Ok::<_, Infallible>(
                service_fn(hello_world)
            )
        }
    });

    let addr = ([127, 0, 0, 1], 3000).into();

    let server = Server::bind(&addr).serve(make_hello_world_svc);

    info!("Listening on http://{}", addr);

    // server.await?;
    if let Err(e) = server.await {
        error!("server error: {}", e);
    }

    Ok(())
}
