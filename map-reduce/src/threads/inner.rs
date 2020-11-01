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

const RETRY_TIMES:usize = 4;
const BOOT_WAIT_DURATION: Duration = Duration::from_millis(200);
const LOCK_WAIT_DURATION: Duration = Duration::from_millis(500);

async fn probe_health <T: Display>(addr: T) -> Result<(), ()> {
    let client = Client::new();
    let probe_addr = format!("http://{}{}",
        addr,
        api::endpoints::HEALTH
    ).parse::<Uri>().unwrap();

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
        thread::sleep(BOOT_WAIT_DURATION);
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
) {
    info!("Waiting for tasks");
    while let Some((task, ack_tx)) = task_funnel.recv().await {
        {
            debug!("Received task {:?}, setting myself Busy", task.task);
            if let Ok(mut state) = state.try_write() {
                state.status = system::Status::Busy;
                ack_tx.send(true).unwrap();
            } else {
                ack_tx.send(false).unwrap();
            }
            info!("Starting execution of {:?}, with input {:?}", task.task, task.input);
            task.execute().await;
            info!("Finished execution of {:?}, with input {:?}", task.task, task.input);
            // Loop until we get the write lock to machine state
            debug!("Finished task {:?}, setting myself Ready", task.task);
            loop {
                debug!("Trying to get write lock on MachineState");
                if let Ok(mut state) = state.try_write() {
                    debug!("Acquired write lock on MachineState");
                    state.status = system::Status::Ready;
                    debug!("Machine set to Ready");
                    break;
                } else {
                    debug!("Couldn't acquire write lock waiting {:?}", LOCK_WAIT_DURATION);
                    thread::sleep(LOCK_WAIT_DURATION);
                }
            }
        }
    }
}

async fn run_pipeline(state: MachineState) {
    // TODO (Optional) Implement a minimum worker threashold
    // TODO Make use of Pipeline
    let workers = {
        let state = state.read().unwrap();
        state.workers.clone()
    };

    // let first_task = tasks::TaskAssignment {
    //     task: tasks::ATask::CountWords,
    //     task_id: 42,
    //     input: vec![tasks::TaskInput {
    //         machine_addr: "http://some.machine".to_string(),
    //         file: "some_file.txt".to_string(),
    //     }],
    // };

    // {
    //     debug!("Assigning tasks to workers");
    //     loop {
    //         debug!("Trying to get read lock on Workers list");
    //         if let Ok(workers) = workers.read() {
    //             debug!("Acquired read lock on Workers");
    //             if workers.len() > 0 {
    //                 debug!("There are workers registered.");
    //                 let mut task_assigned = false;
    //                 for worker in workers.iter() {
    //                     debug!("Assigning {:?} to {:?}", first_task.task, worker.addr);
    //                     if let Ok(task_assign_response) = worker.assign_task(&first_task).await {
    //                         debug!("TASK ASSIGN RESPONSE {:?}", task_assign_response);
    //                         task_assigned = true;
    //                         break;
    //                     } else {
    //                         warn!("Couldn't assign task to worker {:?}, skipping.", worker.addr);
    //                     }
    //                 }
    //                 if task_assigned {
    //                     debug!("Task assigned. Finishing the calculation");
    //                     break;
    //                 } else {
    //                     warn!("Couldn't assign the task yet.");
    //                 }
    //             } else {
    //                 warn!("No workers registered yet to run tasks.");
    //             }
    //         } else {
    //             debug!("Couldn't acquire read lock on workers");
    //         }
    //         debug!("Waiting {:?}", LOCK_WAIT_DURATION);
    //         thread::sleep(LOCK_WAIT_DURATION);
    //     }
    // }
}

pub fn spawn_inner(
    state: MachineState,
    task_funnel: mpsc::Receiver<(
        tasks::TaskAssignment,
        oneshot::Sender<bool>
    )>
) -> JoinHandle<()> {
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
                let state = main_state.read().unwrap();
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
                let mut state = main_state.write().unwrap();
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
                run_pipeline(state).await;
            } else { // Worker
                wait_for_task(state, task_funnel).await;
            }
        });
    }).unwrap()
}

mod tests {
    #[tokio::test]
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
            task_id: 42,
            key: "SomeKey".to_string(),
            input: vec![tasks::TaskInput {
                machine_addr: "http://other_worker.machine".to_string(),
                file: "/some/file".to_string(),
            }]
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
    async fn test_wait_for_task() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use std::collections::HashSet;
        use std::net::SocketAddr;
        use std::sync::{Arc, RwLock};
        use std::time::Instant;

        use tokio::sync::{mpsc, oneshot};
        use hyper::Uri;
        use log::debug;

        use crate::api::system;
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
            RwLock::new(
                Machine {
                    kind: system::MachineKind::Worker,
                    status: system::Status::NotReady,
                    socket: "0.0.0.0:1234".parse::<SocketAddr>().unwrap(),
                    host: "http://worker.machine".to_string(),
                    boot_instant: Instant::now(),
                    master: Some(master),
                    workers: Arc::new(
                        RwLock::new(
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
            task_id: 42,
            key: "SomeKey".to_string(),
            input: vec![tasks::TaskInput {
                machine_addr: "http://other_worker.machine".to_string(),
                file: "/some/file".to_string(),
            }]
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
        // Kick off
        super::wait_for_task(machine_state.clone(), task_rx).await;
        {
            let state = machine_state.read().unwrap();
            // Check if the machine state is back to Ready after running the task.
            assert_eq!(state.status, system::Status::Ready);
        }
    }
}
