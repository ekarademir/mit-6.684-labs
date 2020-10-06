use std::hash::{Hash, Hasher};

use bytes::buf::BufExt as _;
use hyper::{header, body, Body, Client, Request, Uri};
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug, error};

use crate::HostPort;
use super::endpoints;
use super::system;

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum MachineKind {
    Master,
    Worker,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Status {
    Ready,
    Busy,
    NotReady,
    Offline,
    Online,
    Error,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Heartbeat {
    pub kind: MachineKind,
    pub status: Status,
    pub host: String,
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
    pub async fn exchange_heartbeat(&self, host: String, kind: MachineKind, status: Status) -> Status {
        let client = Client::new();
        match self.addr.parse::<Uri>() {
            Ok(uri) => {
                let hb = Heartbeat {
                    kind,
                    status,
                    host,
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

                // let heartbeat: Heartbeat;
                match client.request(req).await {
                    Ok(res) => match body::aggregate(res).await {
                        Ok(response_body) => match serde_json::from_reader::<_, system::HeartbeatResponse> (
                            response_body.reader()
                        ){
                            Ok(heartbeat_response) => {
                                debug!("Received HB response {:?}", heartbeat_response);
                                heartbeat_response.status
                            },
                            Err(e) => {
                                error!("Couldn't parse response: {:?}", e);
                                Status::Error
                            }
                        },
                        Err(e)=> {
                            error!("Couldn't aggregate response body: {:?}", e);
                            Status::Error
                        }
                    },
                    Err(e) => {
                        error!("Couldn't get a response: {:?}", e);
                        Status::Offline
                    }
                }
            },
            Err(e) => {
                error!("Couldn't parse uri: {:?}", e);
                Status::Error
            }
        }
    }
}

impl PartialEq for NetworkNeighbor {
    fn eq(&self, other: &Self) -> bool {
        self.addr == other.addr
    }
}

impl Eq for NetworkNeighbor {}

impl Hash for NetworkNeighbor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.addr.hash(state);
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_sending_heartbeat() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use httptest::{Server, Expectation, matchers::*, responders::*};

        use crate::api::{endpoints, system};
        use crate::api::network_neighbor::NetworkNeighbor;

        let hb_from_master = serde_json::json!(
            {
                "status": "Online"
            }
        );
        let hb_from_worker = serde_json::json!(
            {
                "kind": "Worker",
                "status": "NotReady",
                "host": "http://test.com"
            }
        );

        // Setup server to act as a Master
        let server = Server::run();

        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", endpoints::HEARTBEAT),
                request::body(json_decoded(eq(hb_from_worker)))
            ]).respond_with(
                json_encoded(hb_from_master.clone()
            )),
        );
        let url = server.url("/");
        // Create master NetworkNeighbor
        let test_neighbor = NetworkNeighbor {
            addr: url.to_string(),
            kind: system::MachineKind::Master,
            status: system::Status::NotReady,
            last_heartbeat_ns: 0
        };
        // Run test
        let hb_response = test_neighbor.exchange_heartbeat(
            "http://test.com".to_string(), system::MachineKind::Worker, system::Status::NotReady
        ).await;
        assert!(hb_response == system::Status::Online);
    }

    #[tokio::test]
    async fn test_sending_heartbeat_fail_offline() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use crate::api::system;
        use crate::api::network_neighbor::NetworkNeighbor;

        // Create master NetworkNeighbor
        let test_neighbor = NetworkNeighbor {
            addr: "http://example.local".to_string(),
            kind: system::MachineKind::Master,
            status: system::Status::NotReady,
            last_heartbeat_ns: 0
        };
        // Run test
        let hb_response = test_neighbor.exchange_heartbeat(
            "http://test.com".to_string(), system::MachineKind::Worker, system::Status::NotReady
        ).await;
        assert!(hb_response == system::Status::Offline);
    }
}
