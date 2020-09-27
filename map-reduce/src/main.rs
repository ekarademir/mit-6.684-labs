mod api;
mod errors;
mod workers;

use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use env_logger;
use log::{debug, error, warn};

use api::system;

const DEFAULT_SOCKET: &str = "0.0.0.0:3000";

pub struct Machine {
    kind: system::MachineKind,
    status: system::Status,
    socket: SocketAddr,
}

impl Machine {
    fn new() -> Machine {
        let my_address = if let Ok(master_url) = env::var("MAPREDUCE__ADDRESS") {
            master_url.trim().to_lowercase()
        } else {
            warn!("No address provided, defaulting to {}", DEFAULT_SOCKET);
            String::from(DEFAULT_SOCKET)
        };

        let socket:SocketAddr = if let Ok(addr) = my_address.parse::<SocketAddr>() {
            debug!("Binding socket is {}", addr);
            addr
        } else {
            error!("Can't parse given address for openning up a socket");
            panic!("Can't parse given address for openning up a socket");
        };

        let kind = if let Ok(kind_value) =  env::var("MAPREDUCE__KIND") {
            if kind_value.trim().to_lowercase() == "master" {
                system::MachineKind::Master
            } else {
                system::MachineKind::Worker
            }
        } else {
            system::MachineKind::Worker
        };

        Machine {
            kind,
            status: system::Status::NotReady,
            socket,
        }
    }
}

type MachineState = Arc<Mutex<Machine>>;

fn main() {
    env_logger::Builder::new()
        .filter_module(
            "map_reduce", log::LevelFilter::Debug
        ).init();

    let me: MachineState = Arc::new(
        Mutex::new(
            Machine::new()
        )
    );

    let server_thread = workers::spawn_server(me.clone());
    let client_thread = workers::spawn_client(me.clone());

    client_thread.join().unwrap();
    server_thread.join().unwrap();
}


// TODO: Network discovery
// TODO: master assigns work to workers
