pub mod system;
pub mod endpoints;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use log::{debug};
use hyper::{Body, Method, Request, Response, StatusCode};
use hyper::header;
use hyper::service::Service;

use crate::MachineState;

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

pub struct MainService {
    state: MachineState,
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

        let (
            system_status,
            machine_kind,
            network_urls,
        ) = {
            let state = self.state.lock().unwrap();
            (
                state.status,
                state.kind,
                state.network_urls.clone(),
            )
        };
        Box::pin(async move {
            let result = match (req.method(), req.uri().path()) {
                (&Method::GET, endpoints::HEALTH) => make_result(
                    system::health(
                        machine_kind.clone(),
                        system_status.clone()
                    ).await, Some(StatusCode::OK)
                ),
                (&Method::GET, endpoints::ABOUT) => make_result(
                    system::about(
                        machine_kind.clone(),
                        network_urls.as_ref()
                    ).await, Some(StatusCode::OK)
                ),
                _ => make_result(String::from("{\"error\": \"Not found\"}"), None)
            };
            result
        })
    }
}


pub struct MakeMainService {
    pub state: MachineState,
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
        let state = self.state.clone();
        let main_svc = MainService {
            state,
        };
        let fut = async move { Ok(main_svc) };
        Box::pin(fut)
    }
}
