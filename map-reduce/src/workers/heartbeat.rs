use std::time::Duration;
use std::thread::{self, JoinHandle};

use log::{debug, info, error, warn};
use tokio::runtime::Runtime;
use tokio::sync::oneshot::Receiver;
use tokio::select;

use crate::{MachineState, Workers};
use crate::system;

const SLEEP_DURATION_SEC:u64 = 2;


async fn send_heartbeats(maybe_workers: Option<Workers>, kind: system::MachineKind, status:system::Status) {
    let wait_duration = Duration::from_secs(SLEEP_DURATION_SEC);
    // TODO: if worker: send hb to master, if master send hb to workers
    loop {
        if let Some(workers) = maybe_workers.clone() {
            info!("Sending heartbeat");
            for worker in workers.values() {
                // TODO: Make this concurrent
                worker.send_heartbeat(kind, status).await;
            }
        }
        thread::sleep(wait_duration);
    }
}

pub fn spawn_hearbeat(state: MachineState, kill_rx: Receiver<()>) -> JoinHandle<()> {
    let main_state = state.clone();
    thread::Builder::new().name("Heartbeat".into()).spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async move {
            let (
                my_kind,
                my_status,
                maybe_workers,
            ) = {
                let state = main_state.lock().unwrap();
                (
                    state.kind.clone(),
                    state.status.clone(),
                    state.workers.clone(),
                )
            };

            select! {
                _ = kill_rx => {}
                _ = send_heartbeats(maybe_workers, my_kind, my_status) => {}
            }

        });
    }).unwrap()
}
