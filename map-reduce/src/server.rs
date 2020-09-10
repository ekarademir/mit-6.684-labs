use super::services::{
    master_service,
    worker_service
};

use std::convert::Infallible;
use std::error::Error;

use log::{info, error};
use hyper::service::{make_service_fn, service_fn};
use hyper::{Server};

pub enum MachineType {
    Master,
    Worker
}

pub async fn serve(machine_type: Option<MachineType>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let make_master_service = make_service_fn(|_conn| {
        async {
            Ok::<_, Infallible>(
                service_fn(master_service)
            )
        }
    });

    let make_worker_service = make_service_fn(|_conn| {
        async {
            Ok::<_, Infallible>(
                service_fn(worker_service)
            )
        }
    });

    let addr = ([0, 0, 0, 0], 3000).into();

    let master_server = Server::bind(&addr).serve(make_master_service);
    let worker_server = Server::bind(&addr).serve(make_worker_service);

    let server = match machine_type {
        Some(mtype) => match mtype {
            MachineType::Master =>  {
                master_server
            },
            MachineType::Worker => {
                worker_server
            }
        },
        None => {
            worker_server
        }
    };

    info!("Listening on http://{}", addr);

    if let Err(e) = server.await {
        error!("server error: {}", e);
    }

    Ok(())
}
