pub mod system;
pub mod endpoints;
pub mod network_neighbor;

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use hyper::{Body, Method, Request, Response, StatusCode};
use hyper::header;
use hyper::service::Service;
use tokio::sync::{mpsc, oneshot};

use crate::MachineState;
use crate::tasks;

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
    heartbeat_sender: mpsc::Sender<system::NetworkNeighbor>,
    pub task_sender: mpsc::Sender<(
        tasks::TaskAssignment,
        oneshot::Sender<bool>
    )>,
    pub result_sender: mpsc::Sender<(
        tasks::FinishedTask,
        oneshot::Sender<bool>
    )>
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
        let state = self.state.clone();
        let heartbeat_sender = self.heartbeat_sender.clone();
        let task_sender = self.task_sender.clone();
        let result_sender = self.result_sender.clone();
        Box::pin(async move {
            // TODO HACK, use proper URL parsing lib
            if let Some(pq) = req.uri().path_and_query() {
                if pq.path() == endpoints::CONTENTS_A {
                    if let Some(q) = pq.query() {
                        if q.starts_with("file=") {
                            return make_result(
                                system::contents(
                                    q.strip_prefix("file=").unwrap()
                                ).await,
                                Some(StatusCode::OK)
                            );
                        }
                    }
                }
            }

            let result = match (req.method(), req.uri().path()) {
                (&Method::GET, endpoints::HEALTH) => make_result(
                    system::health(state).await,
                    Some(StatusCode::OK)
                ),
                (&Method::GET, endpoints::ABOUT) => make_result(
                    system::about(state).await,
                    Some(StatusCode::OK)
                ),
                (&Method::POST, endpoints::HEARTBEAT) => make_result(
                    system::heartbeat(req, state, heartbeat_sender).await,
                    Some(StatusCode::OK)
                ),
                (&Method::POST, endpoints::ASSIGN_TASK) => make_result(
                    system::assign_task(req, task_sender).await,
                    Some(StatusCode::OK)
                ),
                (&Method::POST, endpoints::FINISHED_TASK) => make_result(
                    system::finished_task(req, result_sender).await,
                    Some(StatusCode::OK)
                ),
                (_, the_path) => make_result(
                    system::ErrorResponse::not_found(the_path),
                    None
                )
            };
            result
        })
    }
}

pub struct MakeMainService {
    pub state: MachineState,
    pub heartbeat_sender: mpsc::Sender<system::NetworkNeighbor>,
    pub task_sender: mpsc::Sender<(
        tasks::TaskAssignment,
        oneshot::Sender<bool>
    )>,
    pub result_sender: mpsc::Sender<(
        tasks::FinishedTask,
        oneshot::Sender<bool>
    )>
}

impl<T> Service<T> for MakeMainService {
    type Response = MainService;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: T) -> Self::Future {
        let main_svc = MainService {
            state: self.state.clone(),
            heartbeat_sender: self.heartbeat_sender.clone(),
            task_sender: self.task_sender.clone(),
            result_sender: self.result_sender.clone(),
        };
        let fut = async move { Ok(main_svc) };
        Box::pin(fut)
    }
}
