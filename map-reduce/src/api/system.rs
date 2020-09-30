use std::time::Instant;

use bytes::buf::BufExt as _;
use hyper::{header, Body, Client, Method, Request, Uri};
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug};

use crate::errors::CommunicationError;
use crate::MachineState;
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
    Error,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NetworkNeighbor {
    pub addr: String,
    pub kind: MachineKind,
    pub status: Status,
    pub last_heartbeat_ns: u128,
    error: Option<CommunicationError>,
    reason: Option<String>,
}

impl Clone for NetworkNeighbor {
    fn clone(&self) -> NetworkNeighbor {
        NetworkNeighbor {
            addr: self.addr.clone(),
            kind: self.kind.clone(),
            status: self.status.clone(),
            last_heartbeat_ns: self.last_heartbeat_ns.clone(),
            error: None,
            reason: None,
        }
    }
}

impl NetworkNeighbor {
    pub async fn send_heartbeat(&self, kind: MachineKind, status: Status) {
        let client = Client::new();
        let uri:Uri = self.addr.parse().unwrap();
        let hb = Heartbeat {
            kind,
            status
        };
        let req = Request::post(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_string(&hb).unwrap().into())
            .unwrap();

        let hb = client.request(req).await.unwrap();
        // TODO: decide what to do with the response
        debug!("Received HB response {:?}", hb);
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    kind: MachineKind,
    status: Status,
}

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

pub async fn heartbeat(req: Request<Body>) -> String {
    debug!("/heartbeat()");
    let body = hyper::body::aggregate(req).await.unwrap();
    let heartbeat:Heartbeat = serde_json::from_reader(body.reader()).unwrap();
    // serde_json::to_string(&heartbeat).unwrap()
    // TODO: Update worker/master list with heartbeat
    String::from("{\"heartbeat\": \"OK\"}")
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
        if let Some(workers) = &machine_state.workers {
            workers.values().for_each(|worker| {
                network.push(worker.clone());
            });
        }
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

// TODO: Add heartbeat end point https://github.com/hyperium/hyper/blob/master/examples/web_api.rs#L79


// POSSIBLE DEAD CODE
// TODO: Maybe remove this
#[allow(dead_code)]
async fn neighbor_status(url: &String, machine_state: MachineState) -> NetworkNeighbor {
    let parsed_uri = format!("{}{}", url, endpoints::HEALTH).parse::<Uri>();

    match parsed_uri {
        Ok(uri) => {
            debug!("Contacting {:?}", uri);
            let client = Client::new();
            let maybe_contents = client.get(uri).await
                .map(|response| {
                    hyper::body::to_bytes(response.into_body())
                });

            match maybe_contents {
                Ok(content_future) => {
                    match content_future.await {
                        Ok(content) => {
                            let mut de = serde_json::Deserializer::from_reader(content.reader());
                            let neighbor_health = HealthResponse::deserialize(&mut de);

                            match neighbor_health {
                                Ok(health) => {
                                    let last_heartbeat_ns = {
                                        let state = machine_state.lock().unwrap();
                                        Instant::now().duration_since(state.boot_instant).as_nanos()
                                    };
                                    NetworkNeighbor {
                                        addr: url.clone(),
                                        kind: health.kind,
                                        status: health.status,
                                        last_heartbeat_ns,
                                        error: None,
                                        reason: None,
                                    }
                                },
                                Err(e) => {
                                    NetworkNeighbor {
                                        addr: url.clone(),
                                        kind: MachineKind::Unknown,
                                        status: Status::Error,
                                        last_heartbeat_ns: 0,
                                        error: Some(CommunicationError::CantDeserializeResponse),
                                        reason: Some(format!("{:?}", e))
                                    }
                                }
                            }
                        },
                        Err(e) => NetworkNeighbor {
                            addr: url.clone(),
                            kind: MachineKind::Unknown,
                            status: Status::Error,
                            last_heartbeat_ns: 0,
                            error: Some(CommunicationError::CantCreateResponseBytes),
                            reason: Some(format!("{:?}", e))
                        }
                    }
                },
                Err(e) => NetworkNeighbor {
                    addr: url.clone(),
                    kind: MachineKind::Unknown,
                    status: Status::Error,
                    last_heartbeat_ns: 0,
                    error: Some(CommunicationError::CantBufferContents),
                    reason: Some(format!("{:?}", e))
                }
            }
        },
        Err(e) => NetworkNeighbor {
            addr: url.clone(),
            kind: MachineKind::Unknown,
            status: Status::Error,
            last_heartbeat_ns: 0,
            error: Some(CommunicationError::CantParseUrl),
            reason: Some(format!("{:?}", e))
        }
    }
}
