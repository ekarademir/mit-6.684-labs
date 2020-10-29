use std::hash::{Hash, Hasher};

use bytes::buf::BufExt as _;
use hyper::{header, body, Body, Client, Request, Uri};
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug, error};

use crate::HostPort;
use super::endpoints;
use super::system;
use crate::errors;
use crate::tasks;

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

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TaskAssignResponse {
    pub result: tasks::TaskStatus,
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
    pub async fn assign_task(&self, task: &tasks::TaskAssignment) -> Result<TaskAssignResponse, errors::ResponseError> {
        if self.status != Status::Ready {
            return Err(errors::ResponseError::NotReadyYet);
        }
        let client = Client::new();
        let uri = self.addr.parse::<Uri>().unwrap();
        let uri = format!("http://{}{}",
                uri.host_port(),
                endpoints::ASSIGN_TASK
            ).parse::<Uri>().unwrap();
        let req_body = Body::from(serde_json::to_string(&task).unwrap());
        let req = Request::post(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .body(req_body)
            .unwrap();
        match client.request(req).await {
            Ok(res) => match body::aggregate(res).await {
                Ok(response_body) => match serde_json::from_reader::<_, TaskAssignResponse> (
                    response_body.reader()
                ){
                    Ok(task_assign_response) => {
                        debug!("Received response {:?}", task_assign_response);
                        if task_assign_response.result == tasks::TaskStatus::Queued {
                            Ok(task_assign_response)
                        } else {
                            error!(
                                "Worker@{:} not ready yet: {:?}",
                                self.addr,
                                task_assign_response.result
                            );
                            Err(errors::ResponseError::NotReadyYet)
                        }
                    },
                    Err(e) => {
                        error!("Couldn't parse response: {:?}", e);
                        Err(errors::ResponseError::CantParseResponse)
                    }
                },
                Err(e)=> {
                    error!("Couldn't aggregate response body: {:?}", e);
                    Err(errors::ResponseError::CantBufferContents)
                }
            },
            Err(e) => {
                error!("Couldn't get a response: {:?}", e);
                Err(errors::ResponseError::Offline)
            }
        }
    }

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
                // Prep request
                let req = Request::post(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(req_body)
                    .unwrap();
                // Send
                match client.request(req).await {
                    Ok(res) => match body::aggregate(res).await {
                        Ok(response_body) => match serde_json::from_reader::<_, system::HeartbeatResponse> (
                            response_body.reader()
                        ){
                            Ok(heartbeat_response) => {
                                debug!("Received response {:?}", heartbeat_response);
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

    #[tokio::test]
    async fn test_assigning_task() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use httptest::{Server, Expectation, matchers::*, responders::*};

        use crate::api::{endpoints, system};
        use crate::api::network_neighbor::NetworkNeighbor;
        use crate::tasks;

        let task_assignment_response = serde_json::json!(
            {
                "result": "Queued"
            }
        );
        let task_assignment = serde_json::json!(
            {
                "task": "CountWords",
                "task_id": 42,
                "input": [{
                    "machine_addr": "http://some.machine",
                    "file": "some_file.txt"
                }]
            }
        );

        // Setup server to act as a Worker
        let server = Server::run();

        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", endpoints::ASSIGN_TASK),
                request::body(json_decoded(eq(task_assignment)))
            ]).respond_with(
                json_encoded(task_assignment_response.clone()
            )),
        );
        let url = server.url("/");
        // Create worker NetworkNeighbor
        let test_neighbor = NetworkNeighbor {
            addr: url.to_string(),
            kind: system::MachineKind::Worker,
            status: system::Status::Ready,
            last_heartbeat_ns: 0
        };

        let test_task = tasks::TaskAssignment {
            task: tasks::ATask::CountWords,
            input: vec![tasks::TaskInput {
                machine_addr: "http://some.machine".to_string(),
                file: "some_file.txt".to_string(),
            }],
            task_id: 42,
        };

        // Run test
        let response = test_neighbor.assign_task(
            &test_task
        ).await;
        assert_eq!(response.ok(), Some(
            super::TaskAssignResponse {
                result: tasks::TaskStatus::Queued,
            }
        ));

    }
    #[tokio::test]
    async fn test_assigning_task_notready() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use crate::api::system;
        use crate::api::network_neighbor::NetworkNeighbor;
        use crate::errors;
        use crate::tasks;
        // Create worker NetworkNeighbor
        let test_neighbor = NetworkNeighbor {
            addr: "http://worker".to_string(),
            kind: system::MachineKind::Worker,
            status: system::Status::NotReady,
            last_heartbeat_ns: 0
        };

        let test_task = tasks::TaskAssignment {
            task: tasks::ATask::CountWords,
            input: vec![tasks::TaskInput {
                machine_addr: "http://some.machine".to_string(),
                file: "some_file.txt".to_string(),
            }],
            task_id: 42,
        };

        // Run test
        let response = test_neighbor.assign_task(
            &test_task
        ).await;
        assert_eq!(response.err(), Some(errors::ResponseError::NotReadyYet));

    }
    #[tokio::test]
    async fn test_assigning_task_offline() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use crate::api::system;
        use crate::api::network_neighbor::NetworkNeighbor;
        use crate::errors;
        use crate::tasks;
        // Create worker NetworkNeighbor
        let test_neighbor = NetworkNeighbor {
            addr: "http://worker".to_string(),
            kind: system::MachineKind::Worker,
            status: system::Status::Ready,
            last_heartbeat_ns: 0
        };

        let test_task = tasks::TaskAssignment {
            task: tasks::ATask::CountWords,
            input: vec![tasks::TaskInput {
                machine_addr: "http://some.machine".to_string(),
                file: "some_file.txt".to_string(),
            }],
            task_id: 42,
        };

        // Run test
        let response = test_neighbor.assign_task(
            &test_task
        ).await;
        assert_eq!(response.err(), Some(errors::ResponseError::Offline));
    }
}
