use std::time::Instant;

use bytes::buf::BufExt as _;
use hyper::{header, Body, Client, Request, Uri};
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug, info};

use crate::errors::CommunicationError;
use crate::{MachineState, HostPort};
use super::endpoints;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum MachineKind {
    Master,
    Worker,
    Unknown,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum Status {
    Ready,
    Busy,
    NotReady,
    Offline,
    Online,
    Error,
}


#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkNeighbor {
    pub addr: String,
    pub kind: MachineKind,
    pub status: Status,
    pub last_heartbeat_ns: u128,
}

impl Clone for NetworkNeighbor {
    fn clone(&self) -> NetworkNeighbor {
        NetworkNeighbor {
            addr: self.addr.clone(),
            kind: self.kind.clone(),
            status: self.status.clone(),
            last_heartbeat_ns: self.last_heartbeat_ns.clone(),
        }
    }
}

impl NetworkNeighbor {
    pub async fn send_heartbeat(&self, kind: MachineKind, status: Status) {
        let client = Client::new();
        // TODO: decide what to do with the response
        /*
        If can't connect to worker and worker has not been responding for a WHILE (to be determined)
            then drop the worker from list
            - If I am the worker panic and fail/ drop master stop working
        If parsing error, then drop worker immediately
            - If I am worker .......
        */
        if let Ok(uri) = self.addr.parse::<Uri>() {
            let hb = Heartbeat {
                kind,
                status
            };
            let req_body = Body::from(serde_json::to_string(&hb).unwrap());

            let uri = format!("http://{}{}",
                uri.host_port(),
                endpoints::HEARTBEAT
            ).parse::<Uri>().unwrap();

            debug!("Sending request to {:?}", uri);
            let req = Request::post(uri)
                .header(header::CONTENT_TYPE, "application/json")
                .body(req_body)
                .unwrap();

            if let Ok(hb) = client.request(req).await {
                debug!("Received HB response {:?}", hb);
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    kind: MachineKind,
    status: Status,
}

// API Responses
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    kind: MachineKind,
    status: Status,
}

#[derive(Serialize, Deserialize)]
pub struct AboutResponse {
    kind: MachineKind,
    network: Vec<NetworkNeighbor>,
}

#[derive(Serialize, Deserialize)]
pub struct HeartbeatResponse {
    status: Status,
}

#[derive(Serialize, Deserialize)]
pub struct ErrorResponse {
    error: Option<String>,
}

impl ErrorResponse {
    pub fn not_found(item: &str) -> String {
        let resp = ErrorResponse {
            error: Some(format!("{} is not found", item)),
        };
        serde_json::to_string(&resp).unwrap()
    }
    pub fn request_problem(e: String) -> String {
        let resp = ErrorResponse {
            error: Some(format!("Problem with request. {}", e)),
        };
        serde_json::to_string(&resp).unwrap()
    }
}

// API endpoint functions
pub async fn health(state: MachineState) -> String {
    debug!("/health()");
    let (
        status,
        kind,
    ) = {
        let machine_state = state.lock().unwrap();
        (
            machine_state.status.clone(),
            machine_state.kind.clone(),
        )
    };
    let health_response = HealthResponse {
        status,
        kind,
    };

    serde_json::to_string(&health_response).unwrap()
}

pub async fn heartbeat(req: Request<Body>, state: MachineState) -> String {
    debug!("/heartbeat()");
    // let uri = req.uri().clone();
    // TODO correctly get URL of requester
    let uri = req.headers().clone();
    match hyper::body::aggregate(req).await {
        Ok(body) => {
            match serde_json::from_reader::<_, Heartbeat>(body.reader()) {
                Ok(heartbeat) => {
                    let uri = uri.get("host").unwrap();
                    let addr = format!("{}", uri.to_str().unwrap());
                    info!("Heartbeat received from a {:?} at {}", heartbeat.kind, addr);
                    let (
                        my_status,
                        my_kind,
                        boot_instant,
                        workers,
                    ) = {
                        let machine_state = state.lock().unwrap();
                        (
                            machine_state.status.clone(),
                            machine_state.kind.clone(),
                            machine_state.boot_instant.clone(),
                            machine_state.workers.clone(),
                        )
                    };
                    if my_kind == MachineKind::Master {
                        let uri = addr.parse::<Uri>().unwrap();
                        let worker = NetworkNeighbor {
                            addr,
                            status: heartbeat.status,
                            kind: heartbeat.kind,
                            last_heartbeat_ns: Instant::now()
                                .duration_since(boot_instant)
                                .as_nanos(),
                        };
                        match workers.try_lock() {
                            Ok(mut workers) => {
                                debug!("Inserting/updating new worker {}", uri);
                                workers.insert(uri, worker);
                            },
                            Err(e) =>  {
                                debug!("Couldn't acquire write lock to workers. {:?}", e);
                            }
                        }
                        // TODO Add worker register thread / piggyback on hb
                    }
                    let hb = HeartbeatResponse {
                        status: my_status
                    };
                    let resp = serde_json::to_string(&hb).unwrap();
                    debug!("Sending HB respose {}", resp);
                    resp
                },
                Err(e) => {
                    ErrorResponse::request_problem(e.to_string())
                }
            }
        },
        Err(e) => {
            ErrorResponse::request_problem(e.to_string())
        }
    }
}

pub async fn about(state: MachineState) -> String {
    debug!("/about()");
    let (
        kind,
        network
    ) = {
        let machine_state = state.lock().unwrap();
        let mut network: Vec<NetworkNeighbor> = Vec::new();
        if let Some(master) = &machine_state.master {
            network.push(master.clone())
        }
        machine_state.workers.lock().unwrap().values().for_each(|worker| {
            network.push(worker.clone());
        });
        (
            machine_state.kind.clone(),
            network,
        )
    };
    let about_response = AboutResponse {
        kind,
        network,
    };

    serde_json::to_string(&about_response).unwrap()
}
