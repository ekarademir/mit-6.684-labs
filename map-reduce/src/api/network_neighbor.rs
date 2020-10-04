use std::hash::{Hash, Hasher};

use hyper::{header, Body, Client, Request, Uri};
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug};

use crate::HostPort;
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
    pub async fn send_heartbeat(&self, host: String, kind: MachineKind, status: Status) {
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

            if let Ok(hb) = client.request(req).await {
                debug!("Received HB response {:?}", hb);
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
