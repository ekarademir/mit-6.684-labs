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

        if let Ok(worker) = heartbeat_receiver.try_recv() {
            match workers.try_lock() {
                Ok(mut workers) => {
                    debug!("Inserting/updating new worker {}", worker.addr);
                    workers.replace(worker);
                },
                Err(e) =>  {
                    debug!("Couldn't acquire write lock to workers. {:?}", e);
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
    }
}

pub fn spawn_hearbeat(
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
