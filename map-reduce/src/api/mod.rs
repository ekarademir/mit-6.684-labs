pub mod system;
pub mod endpoints;

use log::{debug};
use hyper::{Body, Method, Request, Response, StatusCode};
use hyper::header;
use tokio::sync::{mpsc, watch};

fn json_response(json: String, status: Option<StatusCode>) -> Response<Body> {
    let status = if let Some(code) = status {
        code
    } else {
        StatusCode::NOT_FOUND
    };
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .unwrap()
}


use hyper::service::Service;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct MainService {
    status_sender: mpsc::Sender<system::Status>,
    status_receiver: watch::Receiver<system::Status>,
    status: system::Status,
}

impl Service<Request<Body>> for MainService {
    type Response = Response<Body>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        fn make_result(deserialized_body: String, code: Option<StatusCode>) -> Result<Response<Body>, hyper::Error> {
            Ok(
                json_response(
                    deserialized_body, code
                )
            )
        }
        let system_status = self.status.clone();
        Box::pin(async move {
            let result = match (req.method(), req.uri().path()) {
                (&Method::GET, endpoints::HEALTH) => make_result(system::health(system_status).await, Some(StatusCode::OK)),
                (&Method::GET, endpoints::ABOUT) => make_result(system::about().await, Some(StatusCode::OK)),
                _ => make_result(String::from("{\"error\": \"Not found\"}"), None)
            };
            result
        })
    }
}


pub struct MakeMainService {
    pub status_sender: mpsc::Sender<system::Status>,
    pub status_receiver: watch::Receiver<system::Status>,
}

impl<T> Service<T> for MakeMainService {
    type Response = MainService;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: T) -> Self::Future {
        debug!("Making main service");
        let main_svc = MainService {
            status_sender: self.status_sender.clone(),
            status_receiver: self.status_receiver.clone(),
            status: system::Status::NotReady,
        };
        let fut = async move { Ok(main_svc) };
        Box::pin(fut)
    }
}
