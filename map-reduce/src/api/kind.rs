use std::env;

use hyper::{Body, Response};

#[derive(Debug)]
pub enum MachineKind {
    Master,
    Worker
}

pub fn kind() -> Response<Body> {
    let body = match server_kind() {
        MachineKind::Master => Body::from("I am master"),
        MachineKind::Worker => Body::from("I am worker"),
    };
    Response::new(body)
}


pub fn server_kind() -> MachineKind {
    if env::var("MAPREDUCE__IS_MASTER").is_err() {
        return MachineKind::Worker;
    } else {
        return MachineKind::Master;
    }
}
