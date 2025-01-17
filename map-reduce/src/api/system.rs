use std::time::Instant;

use bytes::buf::BufExt as _;
use hyper::{Body, Request};
use serde::{Deserialize, Serialize};
use serde_json;
use log::{debug, info, error};
use tokio::sync::{mpsc, oneshot};
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::MachineState;
use crate::tasks;

pub use super::network_neighbor::{
    MachineKind,
    Status,
    NetworkNeighbor,
    Heartbeat,
    TaskAssignResponse,
    TaskFinishResponse,
};

// API Responses
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthResponse {
    kind: MachineKind,
    pub status: Status,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ContentResponse {
    pub content: String,
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
    pub fn internal_problem(e: String) -> String {
        let resp = ErrorResponse {
            error: Some(format!("Problem running the request: {}", e)),
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

pub async fn finished_task(
    req: Request<Body>,
    mut result_sender: mpsc::Sender<(
        tasks::FinishedTask,
        oneshot::Sender<bool>
    )>
) -> String {
    debug!("/finished_task()");
    let mut task_finish_response: TaskFinishResponse = TaskFinishResponse {
        result: tasks::FinishReportStatus::Error,
    };
    match hyper::body::aggregate(req).await {
        Ok(body) => {
            match serde_json::from_reader::<_, tasks::FinishedTask>(body.reader()) {
                Ok(result) => {
                    info!("Finished task received: {:?}", result);
                    // Prep ack channel
                    let (ack_tx, ack_rx) = oneshot::channel::<bool>();
                    // Server can get online before task running thread can receive it
                    if let Ok(_) = result_sender.try_send((result, ack_tx)) {
                        if let Ok(true) = ack_rx.await {
                            debug!("Received commit acknowledgement from inner");
                            task_finish_response.result = tasks::FinishReportStatus::Commited;
                            serde_json::to_string(&task_finish_response).unwrap()
                        } else {
                            error!("Didn't receive commit acknowledgement from inner");
                            task_finish_response.result = tasks::FinishReportStatus::Error;
                            serde_json::to_string(&task_finish_response).unwrap()
                        }
                    } else {
                        error!("Can't send result to inner");
                        task_finish_response.result = tasks::FinishReportStatus::Error;
                        serde_json::to_string(&task_finish_response).unwrap()
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

pub async fn contents (
    filename: &str
) -> String {
    debug!("/contents()");
    // TODO HACK get the folder from MachineState, also the entire read folders
    match File::open(
        format!("./data/intermediate/{:}.txt", filename)
    ).await {
        Ok(mut f) => {
            debug!("Reading {:} from intermediate", filename);
            let mut buffer = Vec::new();

            // read the whole file
            f.read_to_end(&mut buffer).await.unwrap();
            // respond with the whole content
            let file_contents = std::str::from_utf8(&buffer).unwrap()
                .trim()
                .to_string();

            let content_response = ContentResponse {
                content: file_contents,
            };
            serde_json::to_string(&content_response).unwrap()
        },
        Err(_) => {
            // Also try reading inputs folder
            match File::open(
                format!("data/inputs/{:}.txt", filename)
            ).await {
                Ok(mut f) => {
                    debug!("Reading {:} from inputs", filename);
                    let mut buffer = Vec::new();

                    // read the whole file
                    f.read_to_end(&mut buffer).await.unwrap();
                    // respond with the whole content
                    let file_contents = std::str::from_utf8(&buffer).unwrap()
                        .trim()
                        .to_string();
                    let content_response = ContentResponse {
                        content: file_contents,
                    };
                    serde_json::to_string(&content_response).unwrap()
                },
                Err(e) => ErrorResponse::internal_problem(e.to_string())
            }
        }
    }

}

#[cfg(test)]
mod tests {
    #[tokio::test]
    #[ignore = "This is broken after we started using ContentResponse"]
    async fn test_endpoint_contents() {
        let response = super::contents("test").await;
        assert_eq!(response, "Some content to test endpoints.".to_string());
    }

    #[tokio::test]
    async fn test_endpoint_finished_task() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use hyper::{Body, Request};
        use tokio::sync::{mpsc, oneshot};
        use crate::tasks;

        let finished_task = serde_json::json!(
            {
                "task_id": 42,
                "task": "CountWords",
                "key": "InputKey",
                "finished": [{
                    "machine_addr": "http://some.machine",
                    "file": "some_file.txt"
                }],
                "result_key": "OutputKey",
                "result": {
                    "machine_addr": "http://some.machine",
                    "file": "some_file.txt"
                }
            }
        );

        // Build Request
        let req: Request<Body> = Request::builder()
            .body(finished_task.to_string().into())
            .unwrap();

        // Build comm channels
        let (result_tx, mut result_rx) = mpsc::channel::<(
            tasks::FinishedTask,
            oneshot::Sender<bool>
        )>(10);

        // Wait result on a new tokio task
        tokio::spawn(async move {
            let response = super::finished_task(req, result_tx.clone()).await;
            let expected_response = serde_json::json!({
                "result": "Commited"
            });
            // Check finishing task returns commited message
            assert_eq!(response, serde_json::to_string(&expected_response).unwrap());
        });

        // Check if the enpoint will send received result to the funnel
        let (received_result, ack_tx) = result_rx.recv().await.unwrap();
        assert_eq!(serde_json::to_value(received_result).unwrap(), finished_task);
        // Send acknowledgement to finish tokio task.
        ack_tx.send(true).unwrap();
    }

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
                "key": "SomeKey",
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

        // Wait task on a new tokio task
        tokio::spawn(async move {
            let response = super::assign_task(req, task_tx.clone()).await;
            let expected_response = serde_json::json!({
                "result": "Queued"
            });
            // Check assigning task returns queued message
            assert_eq!(response, serde_json::to_string(&expected_response).unwrap());
        });

        // Check if the endpoint sends assigned task to the task funnel
        let (assigned_task, ack_tx) = task_rx.recv().await.unwrap();
        assert_eq!(serde_json::to_value(assigned_task).unwrap(), task_assignment);
        // Send acknowledgement to finish tokio task
        ack_tx.send(true).unwrap();
    }
}
