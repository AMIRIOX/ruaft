use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntries;
impl LogEntries {
    pub fn new() -> Self {
        // todo!();
        Self {}
    }
    pub fn append(&mut self) {}
}
