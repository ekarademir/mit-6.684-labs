mod api;
mod errors;
mod threads;
mod tasks;

use std::collections::HashSet;
use std::env;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use env_logger;
use log::{debug, error, warn};
use hyper::Uri;
use tokio::sync::{mpsc, oneshot};

use api::system;

const DEFAULT_SOCKET: &str = "0.0.0.0:3000";

// TODO All this locks can be RwLocks
type MachineState = Arc<Mutex<Machine>>;
type Workers = Arc<Mutex<HashSet<system::NetworkNeighbor>>>;
type HeartbeatKillSwitch = Arc<Mutex<oneshot::Receiver<()>>>;

pub trait HostPort {
    fn host_port(&self) -> String;
}

impl HostPort for Uri {
    fn host_port(&self) -> String {
        let auth = self.clone().into_parts().authority.unwrap();
        if let Some(port) = auth.port() {
            format!(
                "{}:{}",
                auth.host(),
                port
            )
        } else {
            format!(
                "{}",
                auth.host()
            )
        }
    }
}

pub struct Machine {
    kind: system::MachineKind,
    status: system::Status,
    socket: SocketAddr,
    host: String,
    boot_instant: Instant,
    master: Option<system::NetworkNeighbor>,
    workers: Workers,
    master_uri: Option<Uri>,
}

impl Machine {
    fn new() -> Machine {
        let my_address = if let Ok(my_url) = env::var("MAPREDUCE__ADDRESS") {
            my_url.trim().to_lowercase()
        } else {
            warn!("No address provided, defaulting to {}", DEFAULT_SOCKET);
            String::from(DEFAULT_SOCKET)
        };

        let host = if let Ok(my_url) = env::var("MAPREDUCE__HOST") {
            format!(
                "http://{}",
                my_url.parse::<Uri>().unwrap()
                    .host_port()
            )
        } else {
            error!("No host address is provided, panicking");
            panic!("No host address is provided.");
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

        let master_uri = if let Ok(url) = env::var("MAPREDUCE__MASTER") {
            url.trim().to_lowercase().parse::<Uri>().ok()
        } else {
            None
        };

        let workers = Arc::new(
            Mutex::new(
                HashSet::new()
            )
        );

        Machine {
            kind,
            status: system::Status::NotReady,
            socket,
            host,
            boot_instant: Instant::now(),
            master: None,
            workers,
            master_uri,
        }
    }
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
    // Kill trigger to heartbeat loop to shutdown gracefully.
    let (stop_hb_tx, stop_hb_rx) = oneshot::channel::<()>();
    // Heartbeat funnel
    let (heartbeat_tx, heartbeat_rx) = mpsc::channel::<system::NetworkNeighbor>(100);
    // Task funnel
    let (task_tx, task_rx) = mpsc::channel::<(
        tasks::TaskAssignment,
        oneshot::Sender<bool>
    )>(100);

    let heartbeat_kill_sw: HeartbeatKillSwitch = Arc::new(
        Mutex::new(
            stop_hb_rx
        )
    );

    let server_thread = threads::spawn_server(me.clone(), heartbeat_tx, task_tx, kill_rx);
    let inner_thread = threads::spawn_inner(me.clone(), task_rx);
    let heartbeat_thread = threads::spawn_heartbeat(me.clone(), heartbeat_rx, heartbeat_kill_sw);

    if let Err(_) = inner_thread.join() {
        error!("Inner thread panicked, shutting down other threads.");
        kill_tx.send(()).unwrap();
    };
    server_thread.join().unwrap();
    // Stop the heartbeat loop then wait for thread join.
    stop_hb_tx.send(()).unwrap();
    heartbeat_thread.join().unwrap();
}

// TODO handle SIGTERM to kill threads too
// TODO master assigns work to workers
// TODO add test for master, receiving heartbeat from worker

// TODO (Optional) Make it workable via https
