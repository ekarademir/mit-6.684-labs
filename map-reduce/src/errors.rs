use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum CommunicationError {
    NoError,
    CantParseUrl,
    CantBufferContents,
    CantCreateResponseBytes,
    CantDeserializeResponse,
}

#[derive(Debug)]
pub enum SystemError {
    ServerNotResponding
}
