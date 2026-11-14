use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct Command {}
pub struct Client {}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientRequest {}

impl ClientRequest {
    pub fn get_command(&self) -> Command {
        todo!();
        Command {}
    }
}
