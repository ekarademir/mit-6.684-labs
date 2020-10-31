use std::time::Instant;

use bytes::buf::BufExt as _;
use hyper::{Body, Request};
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug, info, error};
use tokio::sync::{mpsc, oneshot};

use crate::MachineState;
use crate::tasks;

pub use super::network_neighbor::{
    MachineKind, Status, NetworkNeighbor, Heartbeat, TaskAssignResponse
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

#[derive(Debug, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub status: Status,
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
        let machine_state = state.read().unwrap();
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

pub async fn heartbeat(
    req: Request<Body>,
    state: MachineState,
    mut heartbeat_sender: mpsc::Sender<NetworkNeighbor>
) -> String {
    debug!("/heartbeat()");
    match hyper::body::aggregate(req).await {
        Ok(body) => {
            match serde_json::from_reader::<_, Heartbeat>(body.reader()) {
                Ok(heartbeat) => {
                    info!("Heartbeat received from a {:?} at {}", heartbeat.kind, heartbeat.host);
                    let (
                        my_status,
                        boot_instant,
                        my_kind,
                        my_host,
                    ) = {
                        let machine_state = state.read().unwrap();
                        (
                            machine_state.status.clone(),
                            machine_state.boot_instant.clone(),
                            machine_state.kind.clone(),
                            machine_state.host.clone(),
                        )
                    };

                    let neighbor = NetworkNeighbor {
                        addr: heartbeat.host,
                        status: heartbeat.status,
                        kind: heartbeat.kind,
                        last_heartbeat_ns: Instant::now()
                            .duration_since(boot_instant)
                            .as_nanos(),
                    };
                    if let Err(e) = heartbeat_sender.send(neighbor).await {
                        error!("Error sending the heartbeat to the heartbeat thread: {}", e);
                    }
                    let hb  = Heartbeat {
                        status: my_status,
                        kind: my_kind,
                        host: my_host,
                    };

                    let resp = serde_json::to_string(&hb).unwrap();
                    debug!("Sending heartbeat response {}", resp);
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
        let machine_state = state.read().unwrap();
        let mut network: Vec<NetworkNeighbor> = Vec::new();
        if let Some(master) = &machine_state.master {
            network.push(master.clone())
        }
        machine_state.workers.read().unwrap().iter().for_each(|worker| {
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

pub async fn assign_task(
    req: Request<Body>,
    mut task_sender: mpsc::Sender<(
        tasks::TaskAssignment,
        oneshot::Sender<bool>
    )>
) -> String {
    debug!("/assign_task()");
    let mut task_assign_response: TaskAssignResponse = TaskAssignResponse {
        result: tasks::TaskStatus::Error,
    };
    match hyper::body::aggregate(req).await {
        Ok(body) => {
            match serde_json::from_reader::<_, tasks::TaskAssignment>(body.reader()) {
                Ok(assignment) => {
                    info!("Task assignment received: {:?}", assignment.task);
                    // Prep ack channel
                    let (ack_tx, ack_rx) = oneshot::channel::<bool>();
                    // Server can get online before task running thread can receive it
                    if let Ok(_) = task_sender.try_send((assignment, ack_tx)) {
                        if let Ok(true) = ack_rx.await {
                            task_assign_response.result = tasks::TaskStatus::Queued;
                            serde_json::to_string(&task_assign_response).unwrap()
                        } else {
                            error!("Didn't receive assignment acknowledgement");
                            task_assign_response.result = tasks::TaskStatus::CantAssign;
                            serde_json::to_string(&task_assign_response).unwrap()
                        }
                    } else {
                        error!("Can't send task to task");
                        task_assign_response.result = tasks::TaskStatus::CantAssign;
                        serde_json::to_string(&task_assign_response).unwrap()
                    }
                },
                Err(e) => {
                    error!("Can't parse task assignment request, {:?}", e);
                    ErrorResponse::request_problem(e.to_string())
                }
            }
        },
        Err(e) => {
            ErrorResponse::request_problem(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_endpoint_assign_task() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use hyper::{Body, Request};
        use tokio::sync::{mpsc, oneshot};
        use crate::tasks;

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

        // Build Request
        let req: Request<Body> = Request::builder()
            .body(task_assignment.to_string().into())
            .unwrap();

        // Build comm channels
        let (task_tx, mut task_rx) = mpsc::channel::<(
            tasks::TaskAssignment,
            oneshot::Sender<bool>
        )>(10);

        // Send task
        tokio::spawn(async move {
            let response = super::assign_task(req, task_tx.clone()).await;
            let expected_response = serde_json::json!({
                "result": "Queued"
            });
            // Check if task is queued
            assert_eq!(response, serde_json::to_string(&expected_response).unwrap());
        });

        // Check assigning task returns queued message
        // Check if task is sent to channel
        let (assigned_task, ack_tx) = task_rx.recv().await.unwrap();
        assert_eq!(serde_json::to_value(assigned_task).unwrap(), task_assignment);
        // Send acknowledgement to finish the return check.
        ack_tx.send(true).unwrap();
    }
}
