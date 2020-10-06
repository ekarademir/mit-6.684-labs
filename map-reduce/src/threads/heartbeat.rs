use std::time::Duration;
use std::thread::{self, JoinHandle};

use log::{debug, info, warn};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::{MachineState, HeartbeatKillSwitch};
use crate::system;

const SLEEP_DURATION_SEC:u64 = 2;


async fn send_heartbeats(
    state: MachineState,
    mut heartbeat_receiver: mpsc::Receiver<system::NetworkNeighbor>,
    kill_rx: HeartbeatKillSwitch,
) {
    let wait_duration = Duration::from_secs(SLEEP_DURATION_SEC);

    loop {
        let (
            kind,
            workers,
            status,
            host,
            maybe_master,
        ) = {
            let my_state = state.lock().unwrap();
            (
                my_state.kind.clone(),
                my_state.workers.clone(),
                my_state.status.clone(),
                my_state.host.clone(),
                my_state.master.clone(),
            )
        };

        if let Ok(_) = kill_rx.lock().unwrap().try_recv() {
            warn!("Kill signal received, stopping heartbeat loop");
            break;
        }

        if let Ok(neighbor) = heartbeat_receiver.try_recv() {
            if neighbor.kind == system::MachineKind::Worker {
                match workers.try_lock() {
                    Ok(mut workers) => {
                        debug!("Inserting/updating new worker {}", neighbor.addr);
                        workers.replace(neighbor);
                    },
                    Err(e) =>  {
                        debug!("Couldn't acquire write lock to workers. {:?}", e);
                    }
                }
            } else if neighbor.kind == system::MachineKind::Master {
                match state.try_lock() {
                    Ok(mut state) => {
                        debug!("Updating master {}", neighbor.addr);
                        state.master = Some(neighbor);
                    },
                    Err(e) =>  {
                        debug!("Couldn't acquire write lock to state. {:?}", e);
                    }
                }

            }
        }

        debug!("Sending heartbeats from {:?}", kind);
        if kind == system::MachineKind::Master {
            if let Ok(workers) = workers.try_lock() {
                if workers.is_empty() {
                    warn!("No workers have been registered yet. Sleeping.");
                } else {
                    info!("Sending heartbeat to workers");
                    for worker in workers.iter() {
                        // TODO: Make this concurrent
                        worker.send_heartbeat(host.clone(), kind, status).await;
                    }
                }
            } else {
                debug!("Couldn't acquire read lock on workers.");
            }

        } else if kind == system::MachineKind::Worker {
            if let Some(master) = maybe_master.clone() {
                info!("Sending heartbeat to master");
                master.send_heartbeat(host.clone(), kind, status).await;
            } else {
                warn!("No master is defined yet. Sleeping.");
            }
        }
        thread::sleep(wait_duration);
        // TODO: decide what to do with the response
        /*
        If can't connect to worker and worker has not been responding for a WHILE (to be determined)
            then drop the worker from list
            - If I am the worker panic and fail/ drop master stop working
        If parsing error, then drop worker immediately
            - If I am worker .......
        */
    }
}

pub fn spawn_heartbeat(
    state: MachineState,
    heartbeat_receiver: mpsc::Receiver<system::NetworkNeighbor>,
    kill_rx: HeartbeatKillSwitch
) -> JoinHandle<()> {
    thread::Builder::new().name("Heartbeat".into()).spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async move {
            send_heartbeats(state.clone(), heartbeat_receiver, kill_rx.clone()).await;
        });
    }).unwrap()
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_heartbeats_from_worker() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use std::collections::HashSet;
        use std::net::SocketAddr;
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};

        use httptest::{Server, Expectation, matchers::*, responders::*};
        use tokio::sync::{mpsc, oneshot};
        use tokio::time::delay_for;

        use crate::api::{endpoints, system};
        use crate::api::network_neighbor::NetworkNeighbor;
        use crate::Machine;

        // Setup server to act as a Master
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", endpoints::HEARTBEAT),
                request::body(json_decoded(eq(serde_json::json!(
                    {
                        "kind": "Worker",
                        "status": "NotReady",
                        "host": "http://test.com"
                    }
                ))))
            ])
            .times(1..)
            .respond_with(status_code(200)),
        );
        let url = server.url("/");
        // form Machine state
        let master = NetworkNeighbor {
            addr: url.to_string(),
            kind: system::MachineKind::Master,
            status: system::Status::NotReady,
            last_heartbeat_ns: 0
        };
        let machine_state = Arc::new(
            Mutex::new(
                Machine {
                    kind: system::MachineKind::Worker,
                    status: system::Status::NotReady,
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
        // Create channels
        let (stop_hb_tx, stop_hb_rx) = oneshot::channel::<()>();
        let (_heartbeat_tx, heartbeat_rx) = mpsc::channel::<system::NetworkNeighbor>(100);
        let heartbeat_kill_sw = Arc::new(
            Mutex::new(
                stop_hb_rx
            )
        );

        // Setup timer for kill switch to end heartbeat loop
        tokio::spawn(async move {
            delay_for(Duration::from_secs(1)).await;
            stop_hb_tx.send(()).unwrap();
        });
        // Run test
        super::send_heartbeats(machine_state, heartbeat_rx, heartbeat_kill_sw).await;
    }

    #[tokio::test]
    async fn test_heartbeats_from_master() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use std::collections::HashSet;
        use std::net::SocketAddr;
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};

        use httptest::{Server, Expectation, matchers::*, responders::*};
        use tokio::sync::{mpsc, oneshot};
        use tokio::time::delay_for;

        use crate::api::{endpoints, system};
        use crate::api::network_neighbor::NetworkNeighbor;
        use crate::Machine;

        // Setup server to act as a Master
        let server = Server::run();
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", endpoints::HEARTBEAT),
                request::body(json_decoded(eq(serde_json::json!(
                    {
                        "kind": "Master",
                        "status": "NotReady",
                        "host": "http://test.com"
                    }
                ))))
            ])
            .times(1..)
            .respond_with(status_code(200)),
        );
        let url = server.url("/");
        // form Machine state
        let worker = NetworkNeighbor {
            addr: url.to_string(),
            kind: system::MachineKind::Worker,
            status: system::Status::NotReady,
            last_heartbeat_ns: 0
        };
        let mut workers_set = HashSet::new();
        workers_set.replace(worker);
        let workers = Arc::new(
            Mutex::new(
                workers_set
            )
        );
        let machine_state = Arc::new(
            Mutex::new(
                Machine {
                    kind: system::MachineKind::Master,
                    status: system::Status::NotReady,
                    socket: "0.0.0.0:3000".parse::<SocketAddr>().unwrap(),
                    host: "http://test.com".to_string(),
                    boot_instant: Instant::now(),
                    master: None,
                    workers,
                    master_uri: Some(url)
                }
            )
        );
        // Create channels
        let (stop_hb_tx, stop_hb_rx) = oneshot::channel::<()>();
        let (_heartbeat_tx, heartbeat_rx) = mpsc::channel::<system::NetworkNeighbor>(100);
        let heartbeat_kill_sw = Arc::new(
            Mutex::new(
                stop_hb_rx
            )
        );

        // Setup timer for kill switch to end heartbeat loop
        tokio::spawn(async move {
            delay_for(Duration::from_secs(1)).await;
            stop_hb_tx.send(()).unwrap();
        });
        // Run test
        super::send_heartbeats(machine_state, heartbeat_rx, heartbeat_kill_sw).await;
    }
}
