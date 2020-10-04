use std::time::Instant;

use bytes::buf::BufExt as _;
use hyper::{Body, Request, Uri};
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug, info};

use crate::MachineState;

pub use super::network_neighbor::{
    MachineKind, Status, NetworkNeighbor, Heartbeat
};

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
    match hyper::body::aggregate(req).await {
        Ok(body) => {
            match serde_json::from_reader::<_, Heartbeat>(body.reader()) {
                Ok(heartbeat) => {
                    info!("Heartbeat received from a {:?} at {}", heartbeat.kind, heartbeat.host);
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
                        let uri = heartbeat.host.parse::<Uri>().unwrap();
                        let worker = NetworkNeighbor {
                            addr: heartbeat.host,
                            status: heartbeat.status,
                            kind: heartbeat.kind,
                            last_heartbeat_ns: Instant::now()
                                .duration_since(boot_instant)
                                .as_nanos(),
                        };
                        match workers.try_lock() {
                            Ok(mut workers) => {
                                debug!("Inserting/updating new worker {}", uri);
                                workers.replace(worker);
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
        machine_state.workers.lock().unwrap().iter().for_each(|worker| {
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
