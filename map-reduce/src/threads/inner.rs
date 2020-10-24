use std::fmt::Display;
use std::net::SocketAddr;
use std::time::Duration;
use std::thread::{self, JoinHandle};

use log::{debug, info, error, warn};
use hyper::{Client, Uri, StatusCode};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};

use crate::api::{self, system};
use crate::MachineState;
use crate::HostPort;
use crate::tasks;

const SLEEP_DURATION_SEC:u64 = 2;
const RETRY_TIMES:usize = 4;

async fn probe_health <T: Display>(addr: T) -> Result<(), ()> {
    let client = Client::new();
    let probe_addr = format!("http://{}{}",
        addr,
        api::endpoints::HEALTH
    ).parse::<Uri>().unwrap();

    let wait_duration = Duration::from_secs(SLEEP_DURATION_SEC);

    for retry in 1..RETRY_TIMES+1 {
        info!("Probing to server {}. Trial {} of {}", probe_addr, retry, RETRY_TIMES);
        let result = client.get(probe_addr.clone()).await;
        match result {
            Ok(response) => {
                match response.status() {
                    StatusCode::OK => {
                        return Ok(());
                    },
                    x => warn!("Server response is {}, will try again.", x),
                }
            },
            _ => warn!("Server did not respond, will try again.")
        }
        thread::sleep(wait_duration);
    }
    error!("Giving up on server wait.");
    Err(())
}

async fn wait_for_server(my_socket: SocketAddr) -> Result<(), ()>{
    probe_health(my_socket).await
}

async fn wait_for_master(master_uri: Uri) -> Result<(), ()>{
    probe_health(master_uri.host_port()).await
}

async fn wait_for_task(
    state: MachineState,
    mut task_funnel: mpsc::Receiver<(
        tasks::TaskAssignment,
        oneshot::Sender<bool>
    )>
) -> Result<(), ()> {
    while let Some((task, ack_tx)) = task_funnel.recv().await {
        {
            if let Ok(mut state) = state.try_lock() {
                state.status = system::Status::Busy;
                ack_tx.send(true).unwrap();
            } else {
                ack_tx.send(false).unwrap();
            }
            // Execute
            task.execute().await;
            // Loop until we get the write lock to machine state
            loop {

            }
        }
    }
    Ok(())
}

pub fn spawn_inner(state: MachineState) -> JoinHandle<()> {
    let main_state = state.clone();
    thread::Builder::new().name("Inner".into()).spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async move {
            // Get socket of this machine
            let (
                my_socket,
                my_kind,
                master_uri,
            ) = {
                let state = main_state.lock().unwrap();
                (
                    state.socket.clone(),
                    state.kind.clone(),
                    state.master_uri.clone(),
                )
            };
            debug!("Waiting for server to come online");
            // Wait until server thread is responding
            match wait_for_server(my_socket).await {
                Ok(_) => info!("Server is online"),
                Err(_) => {
                    error!("Server thread is offline, panicking");
                    panic!("Server thread is offline");
                },
            }
            if my_kind == system::MachineKind::Worker {
                if let Some(master) = master_uri {
                    // Wait until master is responding
                    match wait_for_master(master).await {
                        Ok(_) => info!("Master is online"),
                        Err(_) => {
                            error!("Master machine is offline, panicking");
                            panic!("Master machine is offline");
                        },
                    }
                } else {
                    error!("A worker machine has to have a master machine assigned, panicking");
                    panic!("A worker machine has to have a master machine assigned");
                }
            }
            // Update this machine state as ready
            {
                let mut state = main_state.lock().unwrap();
                state.status = system::Status::Ready;
                if my_kind == system::MachineKind::Worker {
                    state.master = Some(
                        system::NetworkNeighbor {
                            addr: state.master_uri.clone().unwrap().to_string(),
                            kind: system::MachineKind::Master,
                            status: system::Status::Online,
                            last_heartbeat_ns: 0,
                        }
                    );
                }
            }
            // Run tasks
            if my_kind == system::MachineKind::Master  {
                // let workers = {
                //     let state = main_state.lock().unwrap();
                //     state.workers.clone()
                // };

                // let first_task = tasks::TaskAssignment {
                //     task: tasks::ATask::CountWords,
                //     input: tasks::TaskInput {
                //         machine_addr: "http://some.machine".to_string(),
                //         file: "some_file.txt".to_string(),
                //     },
                // };

                // {
                //     for worker in workers.lock().unwrap().iter() {
                //         if let Ok(task_assign_response) = worker.assign_task(&first_task).await {
                //             debug!("{:?}", task_assign_response);
                //             break;
                //         }
                //     }
                // }

            } else { // Worker
                // TODO task receiver will bereated by main
                // thread and will not be dropped until main thread is done hence we can
                // listen to it via `while let`
                // Panic to stop execution? or dont' panic at all.
            }
        });
    }).unwrap()
}

mod tests {
    #[tokio::test]
    #[cfg_attr(feature = "dont_test_this", ignore)]
    async fn test_wait_for_task_listens_until_channel_is_closed() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use tokio::sync::{mpsc, oneshot};
        use log::debug;

        use crate::tasks;

        let (task_tx, _) = mpsc::channel::<(
            tasks::TaskAssignment,
            oneshot::Sender<bool>,
        )>(10);

        let task_asignment = tasks::TaskAssignment {
            task: tasks::ATask::CountWords,
            input: tasks::TaskInput {
                machine_addr: "http://other_worker.machine".to_string(),
                file: "/some/file".to_string(),
            }
        };

        // Run test
        tokio::spawn(async move {
            // Do this several times to test if task receiver stopped or not.
            for _ in 0..3 {
                let (ack_tx, ack_rx) = oneshot::channel::<bool>();
                if let Ok(_) = task_tx.clone().send((task_asignment.clone(), ack_tx)).await {
                    debug!("Task sent");
                    assert!(ack_rx.await.unwrap());
                    debug!("Ack received");
                } else {
                    // Shouldn't reach here
                    assert!(false);
                }
            }
        });
    }
    #[tokio::test]
    // #[cfg_attr(feature = "dont_test_this", ignore)]
    async fn test_wait_for_task() {
        // Uncomment for debugging
        let _ = env_logger::try_init();

        use std::collections::HashSet;
        use std::net::SocketAddr;
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};

        use httptest::{Server, ServerPool, Expectation, matchers::*, responders::*};
        use tokio::sync::{mpsc, oneshot};
        use hyper::Uri;
        use log::debug;

        use crate::api::{endpoints, system};
        use crate::api::network_neighbor::NetworkNeighbor;
        use crate::Machine;
        use crate::tasks;


        // Set up a worker machine that starts listening to tasks
        // Set up the http server to send I'm up message from server thread
        let master_url = "http://master.machine".parse::<Uri>().unwrap();
        let master = NetworkNeighbor {
            addr: master_url.to_string(),
            kind: system::MachineKind::Master,
            status: system::Status::NotReady,
            last_heartbeat_ns: 0
        };

        let machine_state = Arc::new(
            Mutex::new(
                Machine {
                    kind: system::MachineKind::Worker,
                    status: system::Status::NotReady,
                    socket: "0.0.0.0:1234".parse::<SocketAddr>().unwrap(),
                    host: "http://worker.machine".to_string(),
                    boot_instant: Instant::now(),
                    master: Some(master),
                    workers: Arc::new(
                        Mutex::new(
                            HashSet::new()
                        )
                    ),
                    master_uri: Some(master_url)
                }
            )
        );

        let (task_tx, task_rx) = mpsc::channel::<(
            tasks::TaskAssignment,
            oneshot::Sender<bool>,
        )>(10);

        let task_asignment = tasks::TaskAssignment {
            task: tasks::ATask::CountWords,
            input: tasks::TaskInput {
                machine_addr: "http://other_worker.machine".to_string(),
                file: "/some/file".to_string(),
            }
        };

        // Run test
        tokio::spawn(async move {
            let (ack_tx, ack_rx) = oneshot::channel::<bool>();
            if let Ok(_) = task_tx.clone().send((task_asignment.clone(), ack_tx)).await {
                debug!("Task sent");
                assert!(ack_rx.await.unwrap());
                debug!("Ack received");
            } else {
                // Shouldn't reach here
                assert!(false);
            }
        });
        let result = super::wait_for_task(machine_state.clone(), task_rx).await;
        {
            let state = machine_state.lock().unwrap();
            assert_eq!(state.status, system::Status::Busy);
        }

        assert_eq!(result, Ok(()));
    }
}
