mod api;
mod errors;
mod workers;

use std::collections::hash_map::HashMap;
use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use env_logger;
use log::{debug, error, warn};
use tokio::sync::{oneshot};

use api::system;

const DEFAULT_SOCKET: &str = "0.0.0.0:3000";

type MachineState = Arc<Mutex<Machine>>;
type Workers = Arc<HashMap<hyper::Uri, system::NetworkNeighbor>>;
pub struct Machine {
    kind: system::MachineKind,
    status: system::Status,
    socket: SocketAddr,
    boot_instant: Instant,
    master: Option<system::NetworkNeighbor>,
    workers: Option<Workers>,
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
            error!("Can't parse given address for openning up a socket, panicking");
            panic!("Can't parse given address for openning up a socket.");
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
            boot_instant: Instant::now(),
            master: None,
            workers: None,
        }
    }

    // async fn check_neighbors(&mut self) {
    //     let neighbors = system::network(self.network_urls.as_ref()).await;
    //     let workers: Vec<system::NetworkNeighbor> = Vec::new();
    //     neighbors.iter()
    //         .for_each(|neighbor| {
    //             if neighbor.kind == system::MachineKind::Master {

    //             }
    //         });
    // }
}

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

    // Kill trigger to server to shutdown gracefully.
    let (kill_tx, kill_rx) = oneshot::channel::<()>();
    let (stop_hb_tx, stop_hb_rx) = oneshot::channel::<()>();

    let server_thread = workers::spawn_server(me.clone(), kill_rx);
    let inner_thread = workers::spawn_inner(me.clone());
    let heartbeat_thread = workers::spawn_hearbeat(me.clone(), stop_hb_rx);

    if let Err(_) = inner_thread.join() {
        error!("Inner thread panicked, triggering server shutdown");
        kill_tx.send(()).unwrap();
    };
    server_thread.join().unwrap();
    // Stop the heartbeat sending loop
    stop_hb_tx.send(()).unwrap();
    heartbeat_thread.join().unwrap();
}

// TODO master assigns work to workers
// TODO add heartbeat thread
/*
ag komsulari gereksiz, her node kendini biliyor.
eger bir node master ise, yeterince workerin aga katilmasini bekler
eger bir node worker ise, verilen master a heartbeat gonderir, ilk heartbeat ayni zamanda register eder

master node yeterince worker oldukca is yatirmaya devam eder
    yeterli worker node kiritk seviyenin altina duserse yeni is yuklemez, verilen islerin bitimini
    takip eder

*/
