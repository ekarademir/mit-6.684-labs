use std::time::Instant;

use bytes::buf::BufExt as _;
use hyper::{Body, Request};
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug, info, error};
use tokio::sync::mpsc;

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
                        let machine_state = state.lock().unwrap();
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

                    // let hb = HeartbeatResponse {
                    //     status: my_status
                    // };
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

pub async fn assign_task(
    req: Request<Body>,
    state: MachineState,
    mut task_sender: mpsc::Sender<tasks::TaskAssignment>
) -> String {
    debug!("/assign_task()");
    let mut task_assign_response: TaskAssignResponse = TaskAssignResponse {
        result: tasks::TaskStatus::Error,
    };
    // In order to prevent hearbeat thread to signal Ready, immediately set state to Busy
    {
        if let Ok(mut state) = state.try_lock() {
            match state.status {
                Status::Ready => state.status = Status::Busy,
                Status::Busy => {
                    error!("Already running");
                    task_assign_response.result = tasks::TaskStatus::Running;
                    return serde_json::to_string(&task_assign_response).unwrap();
                },
                _ => {
                    error!("Not ready to assign a task.");
                    return ErrorResponse::request_problem(
                        "Not ready to assign a task.".to_string()
                    );
                }
            }
        } else {
            error!("Can't get state lock to set Busy");
            return ErrorResponse::request_problem(
                "Can't get state lock to set Busy".to_string()
            );
        }
    }
    // We set the state to Busy, now try sending the task to task channel
    // Since there is only one master, we are not expecting competing task assignments
    // We are also keeping master on-hold until we can parse the message and send it to
    // the task runner
    match hyper::body::aggregate(req).await {
        Ok(body) => {
            match serde_json::from_reader::<_, tasks::TaskAssignment>(body.reader()) {
                Ok(assignment) => {
                    info!("Task assignment received: {:?}", assignment.task);
                    // Server can get online before task running thread can receive it
                    if let Ok(_) = task_sender.try_send(assignment) {
                        task_assign_response.result = tasks::TaskStatus::Queued;
                        serde_json::to_string(&task_assign_response).unwrap()
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
    #[cfg_attr(feature = "dont_test_this", ignore)]
    async fn test_endpoint_assign_task() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use std::collections::HashSet;
        use std::net::SocketAddr;
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};

        use hyper::{Body, Request, Uri};
        use tokio::sync::{mpsc, oneshot};

        use crate::api::{endpoints, system};
        use crate::api::network_neighbor::NetworkNeighbor;
        use crate::Machine;
        use crate::tasks;

        let task_assignment = serde_json::json!(
            {
                "task": "CountWords",
                "input": {
                    "machine_addr": "http://some.machine",
                    "file": "some_file.txt"
                }
            }
        );

        // Build Request
        let req: Request<Body> = Request::builder()
            .body(task_assignment.to_string().into())
            .unwrap();

        // Build Machine state
        let master = NetworkNeighbor {
            addr: "http://some.machine".to_string(),
            kind: system::MachineKind::Master,
            status: system::Status::NotReady,
            last_heartbeat_ns: 0
        };

        let url = "http://some.machine".parse::<Uri>().unwrap();

        let state = Arc::new(
            Mutex::new(
                Machine {
                    kind: system::MachineKind::Worker,
                    status: system::Status::Ready,
                    socket: "0.0.0.0:3000".parse::<SocketAddr>().unwrap(),
                    host: "http://test.com".to_string(),
                    boot_instant: Instant::now(),
                    master: Some(master),
                    workers: Arc::new(
                        Mutex::new(
                            HashSet::new()
                        )
                    ),
                    master_uri: Some(url)
                }
            )
        );

        // Build comm channels
        let (task_tx, mut task_rx) = mpsc::channel::<tasks::TaskAssignment>(100);

        // Send task
        let response = super::assign_task(req, state.clone(), task_tx.clone()).await;

        let expected_response = serde_json::json!({
            "result": "Queued"
        });

        // Assing task returns queued message
        assert_eq!(response, serde_json::to_string(&expected_response).unwrap());
        // Task is sent to channel
        let assigned_task = task_rx.recv().await.unwrap();
        assert_eq!(serde_json::to_value(assigned_task).unwrap(), task_assignment);
        // Machine state is set to busy
        let status = {
            let state = state.lock().unwrap();
            state.status.clone()
        };
        assert_eq!(status, system::Status::Busy);
    }
}
