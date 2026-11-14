use std::path::Path;

use crate::client::Command;
use crate::raft::TermId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct LogEntry {
    command: Command,
    term_on_recieve: TermId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntries {
    log: Vec<LogEntry>,
}
impl LogEntries {
    pub fn new() -> Self {
        Self { log: Vec::new() }
    }
    pub fn from_single(command: Command, term_on_recieve: TermId) -> Self {
        let entry = LogEntry {
            command,
            term_on_recieve,
        };
        Self { log: vec![entry] }
    }
    pub fn append(&mut self, entry: LogEntry) {
        self.log.push(entry);
    }
    pub fn append_other(&mut self, mut other: Self) {
        self.log.append(&mut other.log);
    }
}

fn storage_path() -> &'static Path {
    Path::new("./")
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Log {
    logs: LogEntries,
}

impl Log {
    pub fn store() {
        todo!();
    }
    pub fn load_or_init() -> Self {
        todo!();
        storage_path().join(Path::new("log"));
        Self {
            logs: LogEntries::new(),
        }
    }
    pub fn append(&mut self, entries: LogEntries) {
        self.logs.append_other(entries);
    }
}
