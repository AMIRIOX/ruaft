use std::{collections::HashMap, net::SocketAddr, time::Duration};

use crate::{
    client::ClientRequest,
    log::{Log, LogEntries},
    net::{Net, OutboundRPCs},
    server::{InboundRPCs, RequestHandler},
};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

pub type NodeId = usize;
pub type TermId = u64;
pub type LogIndex = usize;

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

// pub type RaftResponse = (TermId, bool);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: TermId,
    pub matches: Option<LogIndex>,
    pub source_peer: NodeId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: TermId,
    pub vote_granted: bool,
    pub source_peer: NodeId,
}

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
    total: usize,
}

const HEARTBEAT: Duration = Duration::from_secs(6);
const RATE: u32 = 5;
impl Raft {
    fn generate_random_timeout() -> Duration {
        rand::rng().random_range(HEARTBEAT..=RATE * HEARTBEAT)
    }

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
            role: Role::Follower(Follower {
                last_heartbeat: Instant::now(),
                heartbeat_timeout: Raft::generate_random_timeout(),
            }),
            inbox: rx,
            inbound: RequestHandler::new(tx.clone()),
            outbound: Net::new(id, nodes.clone(), tx.clone()),
            quorum: nodes.len() / 2 + 1,
            total: nodes.len(),
        }
    }
    fn resume() -> Self {
        // read
        todo!();
    }
    async fn apply(&mut self) {
        if self.shared.commit_index > self.shared.last_applied {
            // TODO: apply to state machine
            self.shared.last_applied = self.shared.commit_index;
        }
        todo!();
    }
    async fn poll(&mut self) {
        let mut ticker = tokio::time::interval_at(Instant::now() + HEARTBEAT, HEARTBEAT);
        tokio::select! {
            Some(recv) = self.inbox.recv() => {
                match recv {
                    InboundRPCs::AppendEntries(rpc, tx) => self.handle_append_entries(rpc, tx),
                    InboundRPCs::RequestVote(rpc, tx) => self.handle_request_vote(rpc, tx),
                    InboundRPCs::AppendEntriesResponse(resp) => {
                        self.handle_append_entries_response(resp)
                    }
                    InboundRPCs::RequestVoteResponse(resp) => {
                        self.handle_request_vote_response(resp);
                    }
                    InboundRPCs::ClientRequest(request) => {
                        self.on_received_command(request);
                    }
                }
            },
            _ = ticker.tick() => self.on_tick(),
        }
    }
    fn on_tick(&mut self) {
        match &mut self.role {
            Role::Leader(_) => {
                let prev_log_index = self.shared.log.current_log_index();
                let prev_log_term = self.shared.log.term_at(prev_log_index);

                self.outbound.send_except(
                    self.shared.id,
                    OutboundRPCs::AppendEntries(AppendEntriesRPC {
                        term: self.shared.current_term,
                        leader_id: self.shared.id,
                        prev_log_index,
                        prev_log_term,
                        entries: LogEntries::new(),
                        leader_commit: self.shared.commit_index,
                    }),
                )
            }
            Role::Follower(follower) => {
                if Instant::now() - follower.last_heartbeat > follower.heartbeat_timeout {
                    self.start_election();
                }
            }
            Role::Candidate(candidate) => {
                if Instant::now() > candidate.election_deadline {
                    self.start_election();
                }
            }
        }
    }
    fn on_received_command(&mut self, request: ClientRequest) {
        match &mut self.role {
            Role::Follower(_) | Role::Candidate(_) => {
                todo!();
                // redirect to known leader (otherwise just reject)
            }
            Role::Leader(leader) => {
                let prev_log_index = self.shared.log.current_log_index();
                let prev_log_term = self.shared.log.term_at(prev_log_index);
                let entries =
                    LogEntries::from_single(request.get_command(), self.shared.current_term);
                self.shared.log.append(entries.clone());
                leader.match_index[self.shared.id] = self.shared.log.current_log_index();
                self.outbound.send_except(
                    self.shared.id,
                    OutboundRPCs::AppendEntries(AppendEntriesRPC {
                        term: self.shared.current_term,
                        leader_id: self.shared.id,
                        prev_log_index,
                        prev_log_term,
                        entries: entries,
                        leader_commit: self.shared.commit_index,
                    }),
                );
            }
        }
    }
    fn follow_if_newer(&mut self, term: TermId) {
        if term > self.shared.current_term {
            self.become_follower(term);
            self.shared.voted_for = None;
        }
    }
    fn handle_append_entries(
        &mut self,
        rpc: AppendEntriesRPC,
        reply_tx: oneshot::Sender<AppendEntriesResponse>,
    ) {
        self.follow_if_newer(rpc.term);
        if rpc.term < self.shared.current_term {
            // TODO: result?
            let _ = reply_tx.send(AppendEntriesResponse {
                term: self.shared.current_term,
                matches: None,
                source_peer: self.shared.id,
            });
            return;
        }

        match &mut self.role {
            Role::Follower(follower) => follower.last_heartbeat = Instant::now(),
            Role::Candidate(candidate) => self.become_follower(rpc.term),
            _ => panic!("[FATAL] Leader will never receive AppendEntries RPC!"),
        }

        if let Role::Follower(follower) = &mut self.role {
            follower.last_heartbeat = Instant::now();
        }

        if rpc.prev_log_index > self.shared.log.current_log_index()
            || self.shared.log.term_at(rpc.prev_log_index) != rpc.prev_log_term
        {
            let _ = reply_tx.send(AppendEntriesResponse {
                term: self.shared.current_term,
                matches: None,
                source_peer: self.shared.id,
            });

            return;
        }

        self.shared.log.truncate(rpc.prev_log_index);
        // append all new entries which don't exist in self.log
        self.shared.log.append(rpc.entries);

        if rpc.leader_commit > self.shared.commit_index {
            self.shared.commit_index = rpc.leader_commit.min(self.shared.log.current_log_index());
        }

        let _ = reply_tx.send(AppendEntriesResponse {
            term: self.shared.current_term,
            matches: Some(self.shared.log.current_log_index()),
            source_peer: self.shared.id,
        });
    }
    fn handle_request_vote(
        &mut self,
        rpc: RequestVoteRPC,
        reply_tx: oneshot::Sender<RequestVoteResponse>,
    ) {
        self.follow_if_newer(rpc.term);
        if rpc.term >= self.shared.current_term
            && self.shared.voted_for.is_none_or(|v| v == rpc.candidate_id)
        {
            let last_term = self.shared.log.current_log_term().unwrap_or(0);
            let last_index = self.shared.log.current_log_index();
            if rpc.last_log_term > last_term
                || last_term == rpc.last_log_term && rpc.last_log_index >= last_index
            {
                self.shared.voted_for = Some(rpc.candidate_id);
                match &mut self.role {
                    Role::Candidate(candidate) => {
                        candidate.election_deadline =
                            Instant::now() + Raft::generate_random_timeout()
                    }
                    Role::Follower(follower) => follower.last_heartbeat = Instant::now(),
                    _ => panic!("[FATAL] leader will never grant votes for candidates!"),
                }
                // TODO: result?
                let _ = reply_tx.send(RequestVoteResponse {
                    term: self.shared.current_term,
                    vote_granted: true,
                    source_peer: self.shared.id,
                });

                return;
            }
        }

        // TODO: result?
        let _ = reply_tx.send(RequestVoteResponse {
            term: self.shared.current_term,
            vote_granted: false,
            source_peer: self.shared.id,
        });
    }
    fn advance_commit(&mut self) {
        match &mut self.role {
            Role::Leader(leader) => {
                let mut idx = leader.match_index.clone();
                idx.sort_unstable_by(|a, b| b.cmp(a));
                let advance = idx[self.quorum - 1];
                if advance > self.shared.commit_index
                    && self.shared.log.get(advance).term_on_receive == self.shared.current_term
                {
                    self.shared.commit_index = advance;
                }
            }
            _ => panic!("[WARN] Non-leader calls `advance_commit`, no side effect."),
        }
    }
    fn handle_append_entries_response(&mut self, resp: AppendEntriesResponse) {
        self.follow_if_newer(resp.term);
        if resp.term < self.shared.current_term {
            return;
        }
        if let Role::Leader(leader) = &mut self.role {
            if let Some(matches) = resp.matches {
                if matches > leader.match_index[resp.source_peer] {
                    leader.match_index[resp.source_peer] = matches;
                    leader.next_index[resp.source_peer] = matches + 1;
                    self.advance_commit();
                }
            } else {
                if leader.next_index[resp.source_peer] > 1 {
                    leader.next_index[resp.source_peer] -= 1; // TODO
                }
                let prev_log_index = leader.next_index[resp.source_peer] - 1;
                let prev_log_term = self.shared.log.term_at(prev_log_index);
                self.outbound.send_rpc(
                    resp.source_peer,
                    OutboundRPCs::AppendEntries(AppendEntriesRPC {
                        term: resp.term,
                        leader_id: self.shared.id,
                        prev_log_index,
                        prev_log_term,
                        entries: self.shared.log.slice(
                            leader.next_index[resp.source_peer]
                                ..self.shared.log.current_log_index(),
                        ),
                        leader_commit: self.shared.commit_index,
                    }),
                );
            }
        }
    }
    fn handle_request_vote_response(&mut self, resp: RequestVoteResponse) {
        self.follow_if_newer(resp.term);
        if resp.term < self.shared.current_term {
            return;
        }

        if let Role::Candidate(candidate) = &mut self.role {
            candidate
                .votes
                .entry(resp.source_peer)
                .and_modify(|b| *b = Some(resp.vote_granted));
            if candidate
                .votes
                .iter()
                .filter(|(_, b)| b.is_some_and(|granted| granted))
                .count()
                >= self.quorum
            {
                self.become_leader();
            }
        }
    }

    fn become_follower(&mut self, term: TermId) {
        self.role = Role::Follower(Follower {
            last_heartbeat: Instant::now(),
            heartbeat_timeout: Raft::generate_random_timeout(),
        });
        self.shared.current_term = term;
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader(Leader {
            next_index: Vec::from_fn(self.total, |_| self.shared.log.current_log_index() + 1),
            match_index: Vec::from_fn(self.total, |_| 0),
        });
    }

    fn become_candidate(&mut self) {
        self.role = Role::Candidate(Candidate {
            election_deadline: Instant::now() + Raft::generate_random_timeout(),
            votes: (0..self.total).map(|i| (i, None)).collect(),
        });
    }

    fn start_election(&mut self) {
        self.become_candidate();
        self.shared.current_term += 1;
        self.shared.voted_for = Some(self.shared.id);
        if let Role::Candidate(candidate) = &mut self.role {
            candidate
                .votes
                .entry(self.shared.id)
                .and_modify(|b| *b = Some(true));
        }
        self.outbound.send_except(
            self.shared.id,
            OutboundRPCs::RequestVote(RequestVoteRPC {
                term: self.shared.current_term,
                candidate_id: self.shared.id,
                last_log_index: self.shared.log.current_log_index(),
                last_log_term: self.shared.log.current_log_term().unwrap_or(0),
            }),
        );
    }
}

enum Role {
    Leader(Leader),
    Candidate(Candidate),
    Follower(Follower),
}

pub struct Leader {
    next_index: Vec<LogIndex>,
    match_index: Vec<LogIndex>,
}

pub struct Candidate {
    election_deadline: Instant,
    // TODO: votes:
    votes: HashMap<NodeId, Option<bool>>,
}

pub struct Follower {
    last_heartbeat: Instant,
    heartbeat_timeout: Duration,
}
