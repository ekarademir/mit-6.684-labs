mod api;
mod errors;
mod workers;

use std::thread;

use env_logger;
use tokio::sync::{mpsc, watch};
// use log::{debug, info, error};

use api::system;

struct Machine {
    kind: system::MachineKind,
    status: system::Status,
}

impl Machine {
    fn new(kind: system::MachineKind) -> Machine {
        Machine {
            kind,
            status: system::Status::NotReady,
        }
    }
}

fn main() {
    env_logger::init();

    let me = Machine::new(system::kind());
    let (status_sender_workers, status_receiver_main) = mpsc::channel::<system::Status>(50);
    let (status_sender_main, status_receiver_workers) = watch::channel::<system::Status>(me.status);

    let server_thread = thread::spawn(workers::server_worker());
    let client_thread = thread::spawn(workers::client_worker(
        status_receiver_workers.clone(),
        status_sender_workers.clone()
    ));

    // State worker i geri getir, main thread sadece orchestrator olsun

    client_thread.join().unwrap();
    server_thread.join().unwrap();
}

