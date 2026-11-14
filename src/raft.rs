use std::net::SocketAddr;

use crate::{
    client::ClientRequest,
    log::{Log, LogEntries},
    net::{Net, OutboundRPCs},
    server::{InboundRPCs, RequestHandler},
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub type NodeId = u64;
pub type TermId = u64;
pub type LogIndex = u64;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesRPC {
    pub term: TermId,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: TermId,
    pub entries: LogEntries,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteRPC {
    pub term: TermId,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: TermId,
}

pub type RaftResponse = (TermId, bool);

struct SharedState {
    // Persistent
    id: NodeId, // out of paper, but necessary
    current_term: TermId,
    voted_for: Option<NodeId>,
    log: Log,
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
    quorum: usize,
}

impl Raft {
    fn new(id: NodeId, nodes: Vec<(NodeId, SocketAddr)>) -> Self {
        let (tx, rx) = mpsc::channel(8); // TODO
        Self {
            shared: SharedState {
                id,
                current_term: 1, // TODO: persistent fields, load or init
                voted_for: None,
                log: Log::load_or_init(),
                commit_index: 0,
                last_applied: 1,
            },
            role: Role::Follower,
            inbox: rx,
            inbound: RequestHandler::new(tx.clone()),
            outbound: Net::new(id, nodes.clone(), tx.clone()),
            quorum: nodes.len(),
        }
    }
    fn resume() -> Self {
        // read
        todo!();
    }
    async fn poll(&mut self) {
        while let Some(recv) = self.inbox.recv().await {
            match recv {
                InboundRPCs::AppendEntries(r, tx) => {
                    self.shared.log.append(r.entries);
                }
                InboundRPCs::RequestVote(r, tx) => {
                    todo!();
                }
                InboundRPCs::AppendEntriesResponse((term, success), source_peer) => {
                    todo!();
                }

                InboundRPCs::RequestVoteResponse((term, success), source_peer) => {
                    todo!();
                }
                InboundRPCs::ClientRequest(request) => {
                    self.on_received_command(request);
                }
            }
        }
    }
    fn on_received_command(&mut self, request: ClientRequest) {
        match &mut self.role {
            Role::Follower | Role::Candidate => {
                todo!();
                // redirect to known leader (otherwise just reject)
            }
            Role::Leader(state) => {
                self.shared.log.append(LogEntries::from_single(
                    request.get_command(),
                    self.shared.current_term,
                ));
                self.outbound.send_except(
                    self.shared.id,
                    OutboundRPCs::AppendEntries(AppendEntriesRPC {
                        term: self.shared.current_term,
                        leader_id: self.shared.id,
                        prev_log_index: todo!(),
                        prev_log_term: todo!(),
                        entries: todo!(),
                        leader_commit: self.shared.commit_index,
                    }),
                );
            }
        }
    }
    fn append_entries(&mut self) {
        todo!();
    }
    fn request_vote(&mut self) {
        todo!();
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
