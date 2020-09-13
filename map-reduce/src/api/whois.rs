use std::env;

use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Debug, Serialize, Deserialize)]
pub enum MachineKind {
    Master,
    Worker,
}

#[derive(Serialize, Deserialize)]
pub struct AboutResponse {
    kind: MachineKind,
    version: String,
}

pub fn about() -> String {
    let about_response = AboutResponse {
        kind: kind(),
        version: version(),
    };

    serde_json::to_string(&about_response).unwrap()
}

pub fn kind() -> MachineKind {
    if let Ok(kind_value) =  env::var("MAPREDUCE__KIND") {
        if kind_value.trim().to_lowercase() == "master" {
            return MachineKind::Master;
        } else {
            return MachineKind::Worker;
        }
    } else {
        return MachineKind::Worker;
    };
}

fn version() -> String {
    let ver = "0.1.0";
    return String::from(ver);
}
