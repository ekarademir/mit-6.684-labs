pub mod system;
pub mod endpoints;

use std::convert::Infallible;

use log::{debug};
use hyper::{Body, Method, Request, Response, StatusCode};
use hyper::header;
use tokio::sync::{mpsc, watch};

pub async fn main_service(request: Request<Body>) -> Result<Response<Body>, Infallible> {
    debug!("Building service");

    let response = match (request.method(), request.uri().path()) {
        (&Method::GET, endpoints::ABOUT) => json_response(system::about().await, None),
        (&Method::GET, endpoints::HEALTH) => json_response(system::health().await, None),
        _ => not_found()
    };

    Ok(
        response
    )
}

fn json_response(json: String, status: Option<StatusCode>) -> Response<Body> {
    let status = if let Some(code) = status {
        code
    } else {
        StatusCode::OK
    };
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .unwrap()
}

fn not_found() -> Response<Body> {
    json_response(
        String::from("{\"error\": \"Not found\"}"),  // TODO: Properly serialize this
        Some(StatusCode::NOT_FOUND)
    )
}


use hyper::service::Service;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct MainService {
    status_sender: mpsc::Sender<system::Status>,
    status_receiver: watch::Receiver<system::Status>,
}

impl Service<Request<Body>> for MainService {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        match (req.method(), req.uri().path()) {
            (&Method::GET, endpoints::HEALTH) => Box::pin(async {
                Ok(json_response(system::about().await, None))
            }),
            _ => Box::pin(async {
                Ok(not_found())
            })
        }
    }
}


// pub struct MakeSvc {
//     status_sender: mpsc::Sender<system::Status>,
//     status_receiver: watch::Receiver<system::Status>,
// }

// impl<T> Service<T> for MakeSvc {
//     type Response = MainService;
//     type Error = hyper::Error;
//     type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

//     fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
//         Poll::Ready(Ok(()))
//     }

//     fn call(&mut self, _: T) -> Self::Future {
//         let fut = async move { Ok(
//             MainService {
//                 status_sender: self.status_sender,
//                 status_receiver: self.status_receiver,
//             }
//         ) };
//         Box::pin(fut)
//     }
// }
