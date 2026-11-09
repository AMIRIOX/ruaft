use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntries;
impl LogEntries {
    pub fn new() -> Self {
        todo!();
    }
    pub fn append(&mut self) {}
}
