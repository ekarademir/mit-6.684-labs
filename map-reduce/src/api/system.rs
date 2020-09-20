use std::env;


use bytes::buf::BufExt as _;
use futures::stream::{self, StreamExt};
use hyper::{Client, Uri, StatusCode};
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug};

use crate::errors::CommunicationError;

#[derive(Debug, Serialize, Deserialize)]
pub enum MachineKind {
    Master,
    Worker,
}

#[derive(Debug, Serialize, Deserialize)]
enum Status {
    Ready,
    Busy,
    NotReady,
    Offline,
    Error,
}

#[derive(Debug, Serialize, Deserialize)]
struct NetworkNeighbor {
    addr: String,
    status: Status,
    error: CommunicationError
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    status: Status,
}

#[derive(Serialize, Deserialize)]
pub struct AboutResponse {
    kind: MachineKind,
    version: String,
    network: Option<Vec<NetworkNeighbor>>,
    master: Option<String>,
}

pub fn kind() -> MachineKind {
    if let Ok(kind_value) =  env::var("MAPREDUCE__KIND") {
        if kind_value.trim().to_lowercase() == "master" {
            MachineKind::Master
        } else {
            MachineKind::Worker
        }
    } else {
        MachineKind::Worker
    }
}

fn version() -> String {
    let ver = "0.1.0";
    return String::from(ver);
}

fn master() -> Option<String> {
    if let Ok(master_url) =  env::var("MAPREDUCE__MASTER") {
        Some(master_url.trim().to_lowercase())
    } else {
        None
    }
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
    let parsed_uri = url.parse::<Uri>();

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
                                    status: health.status,
                                    error: CommunicationError::NoError,
                                },
                                _ => NetworkNeighbor {
                                    addr: url,
                                    status: Status::Error,
                                    error: CommunicationError::CantDeserializeResponse,
                                }
                            }
                        },
                        _ => NetworkNeighbor {
                            addr: url,
                            status: Status::Error,
                            error: CommunicationError::CantCreateResponseBytes,
                        }
                    }
                },
                _ => NetworkNeighbor {
                    addr: url,
                    status: Status::Error,
                    error: CommunicationError::CantBufferContents,
                }
            }
        },
        _ => NetworkNeighbor {
            addr: url,
            status: Status::Error,
            error: CommunicationError::CantParseUrl,
        }
    }
}

pub async fn health() -> String {
    debug!("Answering to health()");
    let health_response = HealthResponse {
        status: Status::Ready,
    };

    serde_json::to_string(&health_response).unwrap()
}

pub async fn about() -> String {
    debug!("Answering to about()");
    let about_response = AboutResponse {
        kind: kind(),
        version: version(),
        master: master(),
        network: network().await,
    };

    serde_json::to_string(&about_response).unwrap()
}
