mod api;
mod errors;
mod workers;

use std::thread::{self, JoinHandle};

use env_logger;
use tokio::sync::{mpsc, watch};
use tokio::runtime::Runtime;
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

    let (worker_sender, mut worker_receiver) = mpsc::channel::<system::Status>(50);
    let (state_sender, state_receiver) = watch::channel::<system::Status>(system::Status::NotReady);

    let state_thread = spawn_state(
        worker_receiver,
        state_sender
    );

    let server_thread = workers::spawn_server(
        state_receiver.clone(),
        worker_sender.clone()
    );
    let client_thread = workers::spawn_client(
        state_receiver.clone(),
        worker_sender.clone()
    );

    client_thread.join().unwrap();
    server_thread.join().unwrap();
    state_thread.join().unwrap();
}


fn spawn_state(
    mut worker_receiver: mpsc::Receiver<system::Status>,
    state_sender: watch::Sender<system::Status>
) -> JoinHandle<()> {
    let mut me = Machine::new(system::kind());
    thread::spawn(|| {
        let mut rt = Runtime::new().unwrap();

        rt.block_on(async move {
            loop {
                let status = worker_receiver.recv().await.unwrap();
                me.status = status;
                state_sender.broadcast(me.status.clone());
            }
        });
    })
}
