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
    network: Option<Vec<String>>,
    master: Option<String>,
}

pub fn about() -> String {
    let about_response = AboutResponse {
        kind: kind(),
        version: version(),
        master: master(),
        network: network(),
    };

    serde_json::to_string(&about_response).unwrap()
}

pub fn kind() -> MachineKind {
    if let Ok(kind_value) =  env::var("MAPREDUCE__KIND") {
        if kind_value.trim().to_lowercase() == "master" {
            MachineKind::Master
        } else {
            MachineKind::Worker
        }
    } else {
        MachineKind::Worker
    }
}

fn version() -> String {
    let ver = "0.1.0";
    return String::from(ver);
}

fn master() -> Option<String> {
    // env::var("MAPREDUCE__MASTER").unwrap().trim().to_lowercase()
    if let Ok(master_url) =  env::var("MAPREDUCE__MASTER") {
        Some(master_url.trim().to_lowercase())
    } else {
        None
    }
}

fn network() -> Option<Vec<String>> {
    if let Ok(network_urls) =  env::var("MAPREDUCE__NETWORK") {
        let urls = network_urls.trim()
            .to_lowercase()
            .split(',')
            .map(|url| url.trim()) // Clean
            .filter(|url| !url.is_empty()) // Remove empty
            .map(|url| String::from(url)) // Form String
            .collect();
        Some(urls)
    } else {
        None
    }
}
