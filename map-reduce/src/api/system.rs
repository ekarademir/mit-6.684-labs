use bytes::buf::BufExt as _;
use hyper::{Client, Uri};
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
    error: Option<CommunicationError>,
    reason: Option<String>,
}

impl Clone for NetworkNeighbor {
    fn clone(&self) -> NetworkNeighbor {
        NetworkNeighbor {
            addr: self.addr.clone(),
            kind: self.kind.clone(),
            status: self.status.clone(),
            error: None,
            reason: None,
        }
    }
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

// TODO: Maybe remove this
#[allow(dead_code)]
async fn neighbor_status(url: &String) -> NetworkNeighbor {
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
                                Ok(health) => NetworkNeighbor {
                                    addr: url.clone(),
                                    kind: health.kind,
                                    status: health.status,
                                    error: None,
                                    reason: None,
                                },
                                Err(e) => {
                                    NetworkNeighbor {
                                        addr: url.clone(),
                                        kind: MachineKind::Unknown,
                                        status: Status::Error,
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
                            error: Some(CommunicationError::CantCreateResponseBytes),
                            reason: Some(format!("{:?}", e))
                        }
                    }
                },
                Err(e) => NetworkNeighbor {
                    addr: url.clone(),
                    kind: MachineKind::Unknown,
                    status: Status::Error,
                    error: Some(CommunicationError::CantBufferContents),
                    reason: Some(format!("{:?}", e))
                }
            }
        },
        Err(e) => NetworkNeighbor {
            addr: url.clone(),
            kind: MachineKind::Unknown,
            status: Status::Error,
            error: Some(CommunicationError::CantParseUrl),
            reason: Some(format!("{:?}", e))
        }
    }
}

// API endpoint functions
pub async fn health(state: MachineState) -> String {
    debug!("/health()");
    let (
        kind, status
    ) = {
        let machine_state = state.lock().unwrap();
        (
            machine_state.kind.clone(),
            machine_state.status.clone(),
        )
    };
    let health_response = HealthResponse {
        kind,
        status,
    };

    serde_json::to_string(&health_response).unwrap()
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
            workers.iter().for_each(|worker| {
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
