use crate::raft::TermId;
use crate::{client::Command, raft::LogIndex};
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntry {
    pub command: Command,
    pub term_on_receive: TermId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogEntries {
    log: Vec<LogEntry>,
}
impl LogEntries {
    pub fn new() -> Self {
        Self { log: Vec::new() }
    }
    pub fn from_single(command: Command, term_on_receive: TermId) -> Self {
        let entry = LogEntry {
            command,
            term_on_receive,
        };
        Self { log: vec![entry] }
    }
    pub fn from_vec(drain: Vec<LogEntry>) -> Self {
        Self { log: drain }
    }
    pub fn append(&mut self, entry: LogEntry) {
        self.log.push(entry);
    }
    pub fn append_other(&mut self, mut other: Self) {
        self.log.append(&mut other.log);
    }
    pub fn slice(&self, start: usize, end: usize) -> LogEntries {
        LogEntries::from_vec(self.log[start - 1..end].to_vec())
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
    pub fn get(&self, index: LogIndex) -> &LogEntry {
        debug_assert!(index <= self.current_log_index());
        &self.logs.log[index - 1]
    }
    pub fn term_at(&self, index: LogIndex) -> TermId {
        if index == 0 {
            0 // index = prev_log_index
        } else {
            self.get(index).term_on_receive
        }
    }
    pub fn current_log_index(&self) -> LogIndex {
        self.logs.log.len() as LogIndex
    }
    pub fn current_log_term(&self) -> Option<TermId> {
        self.logs.log.last().map(|e| e.term_on_receive)
    }
    pub fn truncate(&mut self, till: LogIndex) {
        // truncate logs [till + 1, end)
        self.logs.log.truncate(till);
    }

    pub fn slice(&self, index: Range<usize>) -> LogEntries {
        self.logs.slice(index.start, index.end)
    }
}
