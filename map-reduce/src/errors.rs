use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum CommunicationError {
    NoError,
    CantParseUrl,
    CantBufferContents,
    CantCreateResponseBytes,
    CantDeserializeResponse,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TaskAssignError {
    NotReadyYet,
}
