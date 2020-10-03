use std::time::Duration;
use std::thread::{self, JoinHandle};

use log::{debug, info, warn};
use tokio::runtime::Runtime;
use tokio::sync::oneshot::Receiver;
use tokio::select;

use crate::{MachineState, Workers};
use crate::system;

const SLEEP_DURATION_SEC:u64 = 10;


async fn send_heartbeats(
    state: MachineState
) {
    let wait_duration = Duration::from_secs(SLEEP_DURATION_SEC);
    let kind = {
        state.lock().unwrap().kind
    };
    if kind == system::MachineKind::Master {
        debug!("Startig to heartbets to workers from {:?}", kind);
        loop {
            let (
                maybe_workers,
                status,
            ) = {
                let my_state = state.lock().unwrap();
                (
                    my_state.workers.clone(),
                    my_state.status.clone(),
                )
            };
            if let Some(workers) = maybe_workers.clone() {
                info!("Sending heartbeat to workers");
                for worker in workers.lock().unwrap().values() {
                    // TODO: Make this concurrent
                    worker.send_heartbeat(kind, status).await;
                }
            } else {
                warn!("No workers have been registered yet. Sleeping.");
            }
            thread::sleep(wait_duration);
        }
    } else if kind == system::MachineKind::Worker {
        loop {
            let (
                maybe_master,
                status,
            ) = {
                let my_state = state.lock().unwrap();
                (
                    my_state.master.clone(),
                    my_state.status.clone(),
                )
            };
            if let Some(master) = maybe_master.clone() {
                info!("Sending heartbeat to master");
                master.send_heartbeat(kind, status).await;
            } else {
                warn!("No master is defined yet. Sleeping.");
            }
            thread::sleep(wait_duration);
        }
    }
}

pub fn spawn_hearbeat(state: MachineState, kill_rx: Receiver<()>) -> JoinHandle<()> {
    // let main_state = state.clone();
    thread::Builder::new().name("Heartbeat".into()).spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async move {
            select! {
                _ = kill_rx => {
                    warn!("Kill signal received, stopping heartbeat loop");
                }
                _ = send_heartbeats(state) => {}
            }

        });
    }).unwrap()
}

// TODO: Thread loops forever, ignores select, loop on select
