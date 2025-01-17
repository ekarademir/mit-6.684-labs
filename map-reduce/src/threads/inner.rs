use std::fmt::Display;
use std::{fs, env};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use std::thread::{self, JoinHandle};

use log::{debug, info, error, warn};
use bytes::buf::BufExt as _;
use hyper::{body, Client, StatusCode, Uri};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, oneshot};
use tokio::fs::File;
use tokio::io::{self, AsyncWriteExt};

use crate::api::{self, system};
use crate::MachineState;
use crate::HostPort;
use crate::tasks;
use crate::errors;
use crate::api::endpoints;

const RETRY_TIMES:usize = 4;
const BOOT_WAIT_DURATION: Duration = Duration::from_millis(2000);
const LOCK_WAIT_DURATION: Duration = Duration::from_millis(5000);
const PIPELINE_LOOP_SLEEP: Duration = Duration::from_millis(1000);
const MIN_WORKER_THRESHOLD: usize = 2;
const COMMIT_TRY_COUNT: usize = 10;

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

async fn write_file(to: &str, filename: &String, content: String) -> io::Result<()> {
    let path = format!("./data/{:}/{:}.txt", to, filename);
    debug!("Creating {:?}", path);
    let mut buffer = File::create(path).await?;
    debug!("Writing buffer");
    buffer.write_all(content.as_bytes()).await?;
    debug!("Done");
    Ok(())
}

async fn write_intermediate(filename: &String, content: String) -> io::Result<()> {
    write_file("intermediate", filename, content).await
}

async fn write_final(filename: &String, content: String) -> io::Result<()> {
    write_file("outputs", filename, content).await
}

pub async fn request_value(from: tasks::TaskInput) -> Result<String, errors::ResponseError> {
    let client = Client::new();
    let uri = from.machine_addr.parse::<Uri>().unwrap();
    let uri = format!("http://{}{}/?file={}",
            uri.host_port(),
            endpoints::CONTENTS,
            from.file
        ).parse::<Uri>().unwrap();
    match client.get(uri).await {
        Ok(res) => match body::aggregate(res).await {
            Ok(response_body) => match serde_json::from_reader::<_, system::ContentResponse> (
                response_body.reader()
            ){
                Ok(content_response) => {
                    debug!("Received response {:?}", content_response);
                    Ok(content_response.content)
                },
                Err(e) => {
                    error!("Couldn't parse content response: {:?}", e);
                    Err(errors::ResponseError::CantParseResponse)
                }
            },
            Err(e)=> {
                error!("Couldn't aggregate content response body: {:?}", e);
                Err(errors::ResponseError::CantBufferContents)
            }
        },
        Err(e) => {
            error!("Couldn't get a content response: {:?}", e);
            Err(errors::ResponseError::Offline)
        }
    }
}

async fn wait_for_task(
    state: MachineState,
    mut task_funnel: mpsc::Receiver<(
        tasks::TaskAssignment,
        oneshot::Sender<bool>
    )>
) {
    let (
        host,
        boot_instant,
    ) = {
        let state = state.read().unwrap();
        (
            state.host.clone(),
            state.boot_instant.clone(),
        )
    };
    info!("Waiting for tasks");

    // TODO HACK Assign name from the state
    let my_name = {
        let uri = host.clone().parse::<Uri>().unwrap();
        uri.host().unwrap().to_string()
    };

    while let Some((task, ack_tx)) = task_funnel.recv().await {
        {
            let master = {
                let state = state.read().unwrap();
                state.master.clone()
            };
            debug!("Received task {:?}, setting myself Busy", task.task);
            if let Ok(mut state) = state.try_write() {
                state.status = system::Status::Busy;
                ack_tx.send(true).unwrap();
            } else {
                ack_tx.send(false).unwrap();
            }
            info!("Starting execution of {:?}, with input {:?}", task.task, task.input);

            let task_result = task.execute().await;
            let mut results: Vec<tasks::ResultPair> = Vec::new();
            debug!("Execution finished, writing results to disk");
            for (key, value) in task_result {
                let filename = format!(
                    "int_{:}_{:}_{:}",
                    my_name, key, Instant::now().duration_since(boot_instant.clone()).as_micros()
                );
                debug!("Writing result to {:?}", filename);
                match write_intermediate(&filename, value).await {
                    Ok(()) => {
                        results.push(
                            tasks::ResultPair {
                                key,
                                input: tasks::TaskInput {
                                    machine_addr: host.clone(),
                                    file: filename.clone()
                                }
                            }
                        );
                        debug!("Written");
                    },
                    Err(e) => error!("Error writing to {:?}, {:?}", filename, e)
                }
            }
            debug!("Write successful, signaling to master");
            let finished_task = tasks::FinishedTask {
                task: task.task,
                finished: task.input.clone(),
                task_id: task.task_id,
                key: task.key.clone(),
                result: results
            };
            for i in 0..COMMIT_TRY_COUNT {
                info!(
                    "Trying to commit task results for task {:?}...try {:?} of {:?}",
                    task.task, i + 1, COMMIT_TRY_COUNT
                );
                match master.clone().unwrap().finish_task(&finished_task).await {
                    Ok(_) => {
                        info!(
                            "Results of task {:?} written to files and commited to master",
                            task.task
                        );
                        break;
                    },
                    Err(e) => error!("Master did not commit the result, {:?}", e),
                }
                thread::sleep(PIPELINE_LOOP_SLEEP);
            }

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

async fn run_pipeline(
    state: MachineState,
    mut pipeline: tasks::Pipeline,
    mut result_funnel: mpsc::Receiver<(
        tasks::FinishedTask,
        oneshot::Sender<bool>
    )>
) {
    let host = {
        state.read().unwrap().host.clone()
    };
    let pipeline_inputs = init_inputs(&host);
    if pipeline_inputs.len() > 0 {
        debug!("Initiating pipeline");
        pipeline.init(pipeline_inputs);

        debug!("Starting pipeline loop");
        loop {
            debug!("Getting workers list");
            let available_workers = {
                state.read().unwrap()
                    .workers.read().unwrap().clone()
            };
            if available_workers.len() >= MIN_WORKER_THRESHOLD {
                debug!("There are {:?} workers available", available_workers.len());
                let mut finished = false;
                for worker in available_workers {
                    debug!("Getting next assignment for {:?}", worker.addr);
                    match pipeline.next() {
                        tasks::NextTask::Ready(next_task) => {
                            debug!("Assigning {:?} task to {:?}", next_task.task, worker.addr);
                            match worker.assign_task(&next_task).await {
                                Ok(_) => {
                                    info!("Assigned {:?} task to {:?}", next_task.task, worker.addr);
                                    if let Ok(_) = pipeline.update_assignment(
                                        next_task.task_id,
                                        next_task.key,
                                        next_task.input
                                    ) {
                                        debug!("Assignment updated in pipeline");
                                    } else {
                                        error!("Couldn't commit assignment update.");
                                    }
                                },
                                Err(e) => error!("Couldn't assign task, will try next worker {:?}", e)
                            }
                        },
                        tasks::NextTask::Finished => {
                            finished = true;
                            break;
                        },
                        tasks::NextTask::Waiting => {
                            debug!("Pipeline is waiting");
                            break;
                        }
                    }
                }
                if finished {
                    info!("Pipeline is finished, writing outputs");
                    for (key, output) in pipeline.results() {
                        let filename = format!("result_{:}", key);
                        match request_value(output).await {
                            Ok(contents) => match write_final(
                                &filename,
                                contents
                            ).await {
                                Ok(()) => {
                                    info!("Written output file {:?}", filename);
                                },
                                Err(e) => error!("Can't write result to {:}, err: {:?}", filename, e)
                            },
                            Err(e) => error!("Can't write result to {:}, err: {:?}", filename, e)
                        }
                    }
                    break;
                }
                // Consume results queue
                debug!("Trying to receive from finished_task funnel");
                while let Ok((finished_task, ack)) = result_funnel.try_recv() {
                    info!("A task has been finished, {:?} with id {:?}", finished_task.task, finished_task.task_id);
                    let (
                        task_id, finished_key, finished_input
                    ) = {
                        (finished_task.task_id, finished_task.key, finished_task.finished)
                    };
                    let mut all_commited = true;
                    for result in finished_task.result {
                        if let Ok(_) = pipeline.finished_task(
                            task_id,
                            finished_key.clone(),
                            finished_input.clone(),
                            result.key.clone(),
                            result.input
                        ) {
                            debug!("Commited new key {:?}", result.key);
                        } else {
                            all_commited = false;
                            break;
                        }
                    }
                    if all_commited {
                        debug!("Recorded to pipeline");
                        ack.send(true).unwrap();
                    } else {
                        error!("Some results could not be commited.");
                        // TODO this might cause partial results. Decide what to do.
                        ack.send(false).unwrap();
                    }
                }
            } else {
                warn!("Not enough workers to assign work.");
            }
            debug!("Sleeping until next pipeline cycle {:?}", PIPELINE_LOOP_SLEEP);
            thread::sleep(PIPELINE_LOOP_SLEEP);
        }
    } else {
        error!("There are no input files, aborting.");
    }
}

pub fn spawn_inner(
    state: MachineState,
    task_funnel: mpsc::Receiver<(
        tasks::TaskAssignment,
        oneshot::Sender<bool>
    )>,
    task_pipeline: tasks::Pipeline,
    result_funnel: mpsc::Receiver<(
        tasks::FinishedTask,
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
                run_pipeline(state, task_pipeline, result_funnel).await;
            } else { // Worker
                wait_for_task(state, task_funnel).await;
            }
        });
    }).unwrap()
}

fn init_inputs(host: &String) -> tasks::TaskInputs {
    // TODO HACK this entire function is a hack
    info!("Looking for inputs in {:?}/data/inputs", env::current_dir().unwrap());
    let mut inputs = Vec::new();
    match fs::read_dir("./data/inputs") {
        Ok(read_dir) => {
            debug!("Reading contents of the folder");
            for entry in read_dir {
                let path = entry.unwrap().path();
                debug!("Found {:?}", path);
                let filename = path.file_stem().unwrap().to_str().unwrap();
                if filename != ".gitignore" {
                    inputs.push(
                        tasks::TaskInput {
                            machine_addr: host.clone(),
                            file: format!("{:}", filename),
                        }
                    );
                }
            }
        },
        Err(e) => error!("Can't read input folder: {:?}", e)
    }
    inputs
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
