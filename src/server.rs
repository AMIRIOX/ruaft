use crate::client::ClientRequest;
use crate::raft::{AppendEntriesRPC, NodeId, RaftResponse, RequestVoteRPC, TermId};
use futures::StreamExt;
/// Inbound
/// 每个服务器入站的消息
/// RequestHandler
use std::net::SocketAddr;
use tarpc::context::Context;
use tarpc::server::incoming::spawn_incoming;
use tarpc::server::{BaseChannel, incoming::Incoming};
use tarpc::tokio_serde::formats::Json;
use tokio::sync::{mpsc, oneshot};

#[tarpc::service]
pub trait ClientService {
    // TODO
    async fn send_command(request: ClientRequest);
}

pub enum InboundRPCs {
    AppendEntries(AppendEntriesRPC, oneshot::Sender<RaftResponse>),
    RequestVote(RequestVoteRPC, oneshot::Sender<RaftResponse>),
    AppendEntriesResponse(RaftResponse, NodeId),
    RequestVoteResponse(RaftResponse, NodeId),
    ClientRequest(ClientRequest),
}

#[derive(Clone)]
pub struct RequestHandler {
    tx: mpsc::Sender<InboundRPCs>,
}

impl ClientService for RequestHandler {
    async fn send_command(self, _context: Context, request: ClientRequest) {
        self.tx.send(InboundRPCs::ClientRequest(request)).await;
    }
}

#[tarpc::service]
pub trait RaftService {
    // TODO
    async fn append_entries(rpc: AppendEntriesRPC) -> (TermId, bool);
    async fn request_vote(rpc: RequestVoteRPC) -> (TermId, bool);
}

impl RaftService for RequestHandler {
    async fn append_entries(
        self,
        _context: tarpc::context::Context,
        rpc: AppendEntriesRPC,
    ) -> (TermId, bool) {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(InboundRPCs::AppendEntries(rpc, reply_tx))
            .await;
        // SAFETY: not safe
        reply_rx.await.unwrap()
    }
    async fn request_vote(
        self,
        _context: tarpc::context::Context,
        rpc: RequestVoteRPC,
    ) -> (TermId, bool) {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx.send(InboundRPCs::RequestVote(rpc, reply_tx)).await;
        // SAFETY: not safe
        reply_rx.await.unwrap()
    }
}

impl RequestHandler {
    pub fn new(tx: mpsc::Sender<InboundRPCs>) -> Self {
        Self { tx }
    }

    pub async fn start_listen(&self, addr: SocketAddr) -> std::io::Result<()> {
        let listener = tarpc::serde_transport::tcp::listen(addr, Json::default).await?;

        let this = self.clone();
        tokio::spawn(spawn_incoming(
            listener
                .filter_map(|r| async { r.ok() })
                .map(BaseChannel::with_defaults)
                .execute(RaftService::serve(this)),
        ));
        Ok(())
    }
}
