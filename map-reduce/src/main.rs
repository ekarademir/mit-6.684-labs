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
    env_logger::Builder::new()
        .filter_module(
            "map_reduce", log::LevelFilter::Debug
        ).init();

    let server_thread = workers::spawn_server();
    let client_thread = workers::spawn_client();

    client_thread.join().unwrap();
    server_thread.join().unwrap();
}
