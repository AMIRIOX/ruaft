use tokio::sync::mpsc;
use serde::{Deserialize, Serialize};
use crate::client::ClientRequest;
use crate::log::LogEntries;
use crate::net::Net;
use crate::server::RequestHandler;

pub type NodeId = u64;
pub type TermId = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesRPC {
    term: TermId,
    leader_id: NodeId,
    prev_log_index: LogIndex,
    prev_log_term: TermId,
    entries: LogEntries,
    leader_commit: LogIndex,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestForVoteRPC {
    term: TermId,
    candidate_id: NodeId,
    last_log_index: LogIndex,
    last_log_term: TermId,
}

pub type RaftResponse = (TermId, bool);

struct SharedState {
    // Persistent
    current_term: TermId,
    voted_for: Option<NodeId>,
    log: LogEntries,
    // Volatile
    commit_index: LogIndex,
    last_applied: LogIndex,
}
pub struct Raft {
    shared: SharedState,
    role: Role,
    inbound: RequestHandler,
    outbound: Net,
    quoram: usize,
    members: &'static Vec<Raft>,
}

impl Raft {
    fn new(members: &'static Vec<Self>) -> Self {
        let (tx, rx) = mpsc::channel(8); // TODO
        Self {
            shared: SharedState {
                current_term: 1,
                voted_for: None,
                log: LogEntries::new(),
                commit_index: 0,
                last_applied: 1,
            },
            role: Role::Follower,
            inbound: RequestHandler::new(tx),
            outbound: ,
            quoram: members.len(),
            members,
        }
    }
    fn resume() -> Self {
        // read
        todo!();
    }
    fn on_recieved_command(&mut self, request: ClientRequest) {
        match self.role {
            Role::Follower | Role::Candidate => {
                // redirect to known leader (otherwise just reject)
            }
            Role::Leader(state) => {
                // TODO
                self.shared
                    .log
                    .append(LogEntries::new(request.get_command()));
                self.rpc_append_entries();
            }
        }
    }
}

pub enum Role {
    Leader(Leader),
    Candidate,
    Follower,
}

struct Leader {
    // TODO
    next_index: Vec<LogIndex>,
    match_index: Vec<LogIndex>,
}
