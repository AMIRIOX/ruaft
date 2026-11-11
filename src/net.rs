use crate::{
    raft::{AppendEntriesRPC, NodeId, RequestForVoteRPC},
    server::{InboundRPCs, RaftServiceClient},
};
/// Outbound
/// 每个服务器出站的消息
/// n * (n - 1) 的消息拓扑, 对每个出站目标是一个 mpsc::Sender
/// link task 里的 clients
use std::{collections::HashMap, net::SocketAddr, time::Duration};
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::sync::mpsc;

const RECONNECT_BACKOFF: Duration = Duration::from_millis(200);
enum OutboundRPCs {
    AppendEntries(AppendEntriesRPC),
    RequestForVote(RequestForVoteRPC),
}
pub struct Net {
    pub senders: HashMap<NodeId, mpsc::UnboundedSender<OutboundRPCs>>,
}

impl Net {
    pub fn new(
        self_id: NodeId,
        nodes: Vec<(NodeId, SocketAddr)>,
        inbox_tx: mpsc::Sender<InboundRPCs>,
    ) -> Self {
        let mut links = HashMap::new();
        for (id, addr) in nodes {
            if id == self_id {
                continue;
            }

            let (tx, rx) = mpsc::unbounded_channel(); // TODO
            links.insert(id, tx);
            tokio::spawn(link(inbox_tx.clone(), rx, addr, id));
        }
        Self { senders: links }
    }
    pub fn send_rpc(&self, dest_peer: NodeId, rpc: OutboundRPCs) {
        if let Some(tx) = self.senders.get(&dest_peer) {
            tx.send(rpc);
            todo!();
        }
    }
}
async fn link(
    inbox_tx: mpsc::Sender<InboundRPCs>,
    mut rx: mpsc::UnboundedReceiver<OutboundRPCs>,
    addr: SocketAddr,
    dest_peer: NodeId,
) {
    loop {
        if let Ok(connect) = tarpc::serde_transport::tcp::connect(addr, Json::default).await {
            let client = RaftServiceClient::new(client::Config::default(), connect).spawn();
            while let Some(rpc) = rx.recv().await {
                match rpc {
                    OutboundRPCs::AppendEntries(rpc) => {
                        let result = client.append_entries(context::current(), rpc).await;
                        if let Ok(result) = result {
                            inbox_tx
                                .send(InboundRPCs::AppendEntriesResponse(result, dest_peer))
                                .await;
                        } else {
                            break;
                        }
                    }
                    OutboundRPCs::RequestForVote(rpc) => {
                        let result = client.request_for_vote(context::current(), rpc).await;
                        if let Ok(result) = result {
                            inbox_tx
                                .send(InboundRPCs::RequestForVoteResponse(result, dest_peer))
                                .await;
                        } else {
                            break;
                        }
                    }
                }
            }
        }
        if rx.is_closed() {
            return;
        }
        tokio::time::sleep(RECONNECT_BACKOFF).await;
    }
}
