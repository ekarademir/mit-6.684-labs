use std::env;

use bytes::buf::BufExt as _;
use futures::stream::{StreamExt};
use hyper::{Client, Uri};
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug};

use crate::errors::CommunicationError;
use super::endpoints;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
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
struct NetworkNeighbor {
    addr: String,
    kind: MachineKind,
    status: Status,
    error: Option<CommunicationError>,
    reason: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    kind: MachineKind,
    status: Status,
}

#[derive(Serialize, Deserialize)]
pub struct AboutResponse {
    kind: MachineKind,
    version: String,
    network: Option<Vec<NetworkNeighbor>>,
}

fn version() -> String {
    let ver = "0.1.0";
    return String::from(ver);
}

async fn network() -> Option<Vec<NetworkNeighbor>> {
    if let Ok(network_urls) =  env::var("MAPREDUCE__NETWORK") {
        let urls = network_urls.trim()
            .to_lowercase()
            .split(',')
            .map(|url| url.trim()) // Clean
            .filter(|url| !url.is_empty()) // Remove empty
            .map(|url| String::from(url)) // Form String
            .collect::<Vec<String>>();

        let mut neighbor_pings = FuturesUnordered::new();
        for url in urls {
            neighbor_pings.push(neighbor_status(url));
        }

        let mut neighbors: Vec<NetworkNeighbor> = Vec::new();
        while let Some(ping_result) = neighbor_pings.next().await {
            neighbors.push(ping_result);
        }

        Some(neighbors)
    } else {
        None
    }
}

async fn neighbor_status(url: String) -> NetworkNeighbor {
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
                                    addr: url,
                                    kind: health.kind,
                                    status: health.status,
                                    error: None,
                                    reason: None,
                                },
                                Err(e) => {
                                    NetworkNeighbor {
                                        addr: url,
                                        kind: MachineKind::Unknown,
                                        status: Status::Error,
                                        error: Some(CommunicationError::CantDeserializeResponse),
                                        reason: Some(format!("{:?}", e))
                                    }
                                }
                            }
                        },
                        Err(e) => NetworkNeighbor {
                            addr: url,
                            kind: MachineKind::Unknown,
                            status: Status::Error,
                            error: Some(CommunicationError::CantCreateResponseBytes),
                            reason: Some(format!("{:?}", e))
                        }
                    }
                },
                Err(e) => NetworkNeighbor {
                    addr: url,
                    kind: MachineKind::Unknown,
                    status: Status::Error,
                    error: Some(CommunicationError::CantBufferContents),
                    reason: Some(format!("{:?}", e))
                }
            }
        },
        Err(e) => NetworkNeighbor {
            addr: url,
            kind: MachineKind::Unknown,
            status: Status::Error,
            error: Some(CommunicationError::CantParseUrl),
            reason: Some(format!("{:?}", e))
        }
    }
}

// API endpoint functions
pub async fn health(kind: MachineKind, status: Status) -> String {
    debug!(
        "/health({:?}, {:?})",
        kind, status
    );
    let health_response = HealthResponse {
        kind,
        status,
    };

    serde_json::to_string(&health_response).unwrap()
}

pub async fn about(kind: MachineKind) -> String {
    debug!(
        "/about({:?})",
        kind
    );
    let about_response = AboutResponse {
        kind,
        version: version(),
        network: network().await,
    };

    serde_json::to_string(&about_response).unwrap()
}
