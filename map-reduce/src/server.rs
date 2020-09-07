
use std::convert::Infallible;
use std::error::Error;

use log::{info, error};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};

async fn hello_world(_: Request<Body>) -> Result<Response<Body>, Infallible> {
    info!("Responding to hello_world request");
    Ok(
        Response::new(
            Body::from(
                "Hello World!"
            )
        )
    )
}

pub async fn serve() -> Result<(), Box<dyn Error + Send + Sync>> {
    let make_hello_world_svc = make_service_fn(|_conn| {
        async {
            Ok::<_, Infallible>(
                service_fn(hello_world)
            )
        }
    });

    let addr = ([0, 0, 0, 0], 3000).into();

    let server = Server::bind(&addr).serve(make_hello_world_svc);

    info!("Listening on http://{}", addr);

    if let Err(e) = server.await {
        error!("server error: {}", e);
    }

    Ok(())
}
