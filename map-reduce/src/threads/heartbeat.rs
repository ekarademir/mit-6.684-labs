use std::time::Duration;
use std::thread::{self, JoinHandle};

use log::{debug, error, info, warn};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;

use crate::{MachineState, HeartbeatKillSwitch};
use crate::system;

const WAIT_DURATION:Duration = Duration::from_millis(100);
const NEIGHTBOR_DROP_THRESHOLD:u128 = 5_000_000_000; // ns


async fn send_heartbeats(
    state: MachineState,
    mut heartbeat_receiver: mpsc::Receiver<system::NetworkNeighbor>,
    kill_rx: HeartbeatKillSwitch,
) {

    loop {
        let (
            kind,
            workers,
            status,
            host,
            maybe_master,
            boot_time,
        ) = {
            let my_state = state.lock().unwrap();
            (
                my_state.kind.clone(),
                my_state.workers.clone(),
                my_state.status.clone(),
                my_state.host.clone(),
                my_state.master.clone(),
                my_state.boot_instant.clone(),
            )
        };

        let since_boot = boot_time.elapsed().as_nanos();

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
            if let Ok(mut workers) = workers.try_lock() {
                if workers.is_empty() {
                    warn!("No workers have been registered yet. Sleeping.");
                } else {
                    info!("Sending heartbeat to workers");
                    let mut drop_workers: Vec<system::NetworkNeighbor> = Vec::new();
                    // TODO: Make this concurrent
                    for worker in workers.iter() {
                        match worker.exchange_heartbeat(host.clone(), kind, status).await {
                            system::Status::Error
                            | system::Status::Offline
                            | system::Status::NotReady => {
                                if since_boot - worker.last_heartbeat_ns > NEIGHTBOR_DROP_THRESHOLD {
                                    drop_workers.push(worker.clone());
                                }
                            },
                            _ => {}
                        }
                    }
                    for dropping_worker in drop_workers.iter() {
                        workers.remove(dropping_worker);
                    }
                }
            } else {
                debug!("Couldn't acquire read lock on workers.");
            }

        } else if kind == system::MachineKind::Worker {
            if let Some(master) = maybe_master.clone() {
                info!("Sending heartbeat to master");
                match master.exchange_heartbeat(host.clone(), kind, status).await {
                    system::Status::Error
                    | system::Status::Offline
                    | system::Status::NotReady => {
                        if since_boot - master.last_heartbeat_ns > NEIGHTBOR_DROP_THRESHOLD {
                            let threshold: f32 = NEIGHTBOR_DROP_THRESHOLD as f32 / 1_000_000_000.0;
                            error!("Master machine has not been ready for {} secs, panicking", threshold);
                            panic!("Master machine has not been ready for {} secs", threshold);
                        }
                    },
                    _ => {}
                }
            } else {
                warn!("No master is defined yet. Sleeping.");
            }
        }
        thread::sleep(WAIT_DURATION);
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
    #[cfg_attr(feature = "dont_test_this", ignore)]
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

        let hb_from_master = serde_json::json!(
            {
                "status": "Ready"
            }
        );

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
            .respond_with(
                json_encoded(hb_from_master.clone()
            )),
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
    #[cfg_attr(feature = "dont_test_this", ignore)]
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

        let hb_from_master = serde_json::json!(
            {
                "status": "Ready"
            }
        );

        // Setup server to act as a Worker
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
            .respond_with(
                json_encoded(hb_from_master.clone()
            )),
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
                    master_uri: None
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
    #[cfg_attr(feature = "dont_test_this", ignore)]
    #[should_panic]
    async fn test_heartbeats_from_worker_notready_master() {
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
        // Response to heartbeat from a worker
        // will send NotReady heartbeat everytime
        let hb_from_master = serde_json::json!(
            {
                "status": "NotReady"
            }
        );
        server.expect(
            Expectation::matching(all_of![
                request::method_path("POST", endpoints::HEARTBEAT),
                request::body(json_decoded(eq(serde_json::json!(
                    {
                        "kind": "Worker",
                        "status": "Online",
                        "host": "http://test.com"
                    }
                ))))
            ])
            .times(1..)
            .respond_with(
                json_encoded(hb_from_master.clone()
            )),
        );
        let url = server.url("/");
        // form Machine state
        let boot = Instant::now();

        let master = NetworkNeighbor {
            addr: url.to_string(),
            kind: system::MachineKind::Master,
            status: system::Status::NotReady, // Master was not ready
            last_heartbeat_ns: boot.elapsed().as_nanos()
        };
        let machine_state = Arc::new(
            Mutex::new(
                Machine {
                    kind: system::MachineKind::Worker,
                    status: system::Status::Online,
                    socket: "0.0.0.0:3000".parse::<SocketAddr>().unwrap(),
                    host: "http://test.com".to_string(),
                    boot_instant: boot,
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
        // It should panic before this is triggered
        tokio::spawn(async move {
            delay_for(Duration::from_secs(6)).await;
            stop_hb_tx.send(()).unwrap();
        });
        // Run test
        super::send_heartbeats(machine_state, heartbeat_rx, heartbeat_kill_sw).await;
    }

    #[tokio::test]
    #[cfg_attr(feature = "dont_test_this", ignore)]
    // TODO investigate this test
    #[ignore = "Does not pass this test all the time"]
    #[should_panic]
    async fn test_heartbeats_from_worker_offline_master() {
        // Uncomment for debugging
        // let _ = env_logger::try_init();

        use std::collections::HashSet;
        use std::net::SocketAddr;
        use std::sync::{Arc, Mutex};
        use std::time::{Duration, Instant};

        use tokio::sync::{mpsc, oneshot};
        use tokio::time::delay_for;
        use hyper::Uri;

        use crate::api::system;
        use crate::api::network_neighbor::NetworkNeighbor;
        use crate::Machine;

        // form Machine state
        let boot = Instant::now();
        let master_url = "http://example.local";
        let master_uri = "http://example.local".parse::<Uri>().unwrap();
        let master = NetworkNeighbor {
            addr: master_url.to_string(),
            kind: system::MachineKind::Master,
            status: system::Status::NotReady, // Master was not ready
            last_heartbeat_ns: boot.elapsed().as_nanos()
        };
        let machine_state = Arc::new(
            Mutex::new(
                Machine {
                    kind: system::MachineKind::Worker,
                    status: system::Status::Online,
                    socket: "0.0.0.0:3000".parse::<SocketAddr>().unwrap(),
                    host: "http://test.com".to_string(),
                    boot_instant: boot,
                    master: Some(master),
                    workers: Arc::new(
                        Mutex::new(
                            HashSet::new()
                        )
                    ),
                    master_uri: Some(master_uri)
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
        // It should panic before this is triggered
        tokio::spawn(async move {
            delay_for(Duration::from_secs(7)).await;
            stop_hb_tx.send(()).unwrap();
        });
        // Run test
        super::send_heartbeats(machine_state, heartbeat_rx, heartbeat_kill_sw).await;
    }
}
