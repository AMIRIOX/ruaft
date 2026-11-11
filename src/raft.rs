use std::net::SocketAddr;

use crate::{
    client::ClientRequest,
    log::LogEntries,
    net::Net,
    server::{InboundRPCs, RequestHandler},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

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
    id: NodeId, // out of paper, but necessary
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
    inbox: mpsc::Receiver<InboundRPCs>,
    inbound: RequestHandler,
    outbound: Net,
    quoram: usize,
}

impl Raft {
    fn new(id: NodeId, nodes: Vec<(NodeId, SocketAddr)>) -> Self {
        let (tx, rx) = mpsc::channel(8); // TODO
        Self {
            shared: SharedState {
                id,
                current_term: 1,
                voted_for: None,
                log: LogEntries::new(),
                commit_index: 0,
                last_applied: 1,
            },
            role: Role::Follower,
            inbox: rx,
            inbound: RequestHandler::new(tx.clone()),
            outbound: Net::new(id, nodes.clone(), tx.clone()),
            quoram: nodes.len(),
        }
    }
    fn resume() -> Self {
        // read
        todo!();
    }
    fn on_recieved_command(&mut self, request: ClientRequest) {
        match &mut self.role {
            Role::Follower | Role::Candidate => {
                // redirect to known leader (otherwise just reject)
            }
            Role::Leader(state) => {
                // TODO
                /*
                self.shared
                    .log
                    .append(LogEntries::new(request.get_command()));
                self.outbound.send_rpc(dest_peer, rpc);
                */
            }
        }
    }
}

enum Role {
    Leader(Leader),
    Candidate,
    Follower,
}

pub struct Leader {
    // TODO
    next_index: Vec<LogIndex>,
    match_index: Vec<LogIndex>,
}
