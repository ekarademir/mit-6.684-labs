use std::time::Duration;
use std::thread::{self, JoinHandle};

use log::{debug, info, warn};
use tokio::runtime::Runtime;

use crate::{MachineState, HeartbeatKillSwitch};
use crate::system;

const SLEEP_DURATION_SEC:u64 = 2;


async fn send_heartbeats(
    state: MachineState,kill_rx: HeartbeatKillSwitch
) {
    let wait_duration = Duration::from_secs(SLEEP_DURATION_SEC);
    let kind = {
        state.lock().unwrap().kind
    };
    loop {
        if let Ok(_) = kill_rx.lock().unwrap().try_recv() {
            warn!("Kill signal received, stopping heartbeat loop");
            break;
        }
        debug!("Starting heartbeats from {:?}", kind);
        if kind == system::MachineKind::Master {
            let (
                workers_ref,
                status,
            ) = {
                let my_state = state.lock().unwrap();
                (
                    my_state.workers.clone(),
                    my_state.status.clone(),
                )
            };
            let workers = workers_ref.lock().unwrap();
            if workers.is_empty() {
                warn!("No workers have been registered yet. Sleeping.");
            } else {
                info!("Sending heartbeat to workers");
                for worker in workers.values() {
                    // TODO: Make this concurrent
                    worker.send_heartbeat(kind, status).await;
                }
            }
            thread::sleep(wait_duration);
        } else if kind == system::MachineKind::Worker {
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

pub fn spawn_hearbeat(state: MachineState, kill_rx: HeartbeatKillSwitch) -> JoinHandle<()> {
    thread::Builder::new().name("Heartbeat".into()).spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async move {
            send_heartbeats(state.clone(), kill_rx.clone()).await;
        });
    }).unwrap()
}
