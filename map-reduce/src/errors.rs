use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum CommunicationError {
    NoError,
    CantParseUrl,
    CantBufferContents,
    CantCreateResponseBytes,
    CantDeserializeResponse,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ResponseError {
    NotReadyYet,
    CantParseResponse,
    CantBufferContents,
    Offline,
}
