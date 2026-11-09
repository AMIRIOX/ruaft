use serde::{Deserialize, Serialize};

pub struct Client {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientRequest {}

impl ClientRequest {
    pub fn get_command() {}
}
