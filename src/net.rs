use crate::{
    raft::{AppendEntriesRPC, LogIndex, NodeId, RaftResponse, RequestForVoteRPC, TermId},
    server::{InboundRPCs, RaftService, RaftServiceClient},
};
/// Outbound
/// 每个服务器出站的消息
/// n * (n - 1) 的消息拓扑, 对每个出站目标是一个 mpsc::Sender
/// link task 里的 clients
use std::{collections::HashMap, net::SocketAddr};
use tarpc::{client, context, tokio_serde::formats::Json};
use tokio::sync::{mpsc, oneshot};

enum OutboundRPCs {
    AppendEntries(AppendEntriesRPC),
    RequestForVote(RequestForVoteRPC),
}
pub struct Net {
    pub senders: HashMap<NodeId, mpsc::Sender<OutboundRPCs>>,
}

impl Net {
    pub fn new(self_id: NodeId, quoram: usize) -> Self {
        let mut links = HashMap::new();
        for id in 1..=quoram as NodeId {
            if id == self_id {
                continue;
            }

            let (tx, rx) = mpsc::channel(8); // TODO
            links.insert(id, tx.clone());
            link(, rx, addr);
        }
        Self { senders: links }
    }
}
async fn link(
    mut inbox_tx: mpsc::Sender<InboundRPCs>,
    mut rx: mpsc::Receiver<OutboundRPCs>,
    nodes: Vec<SocketAddr>,
) {
    let mut clients = Vec::new();
    for addr in nodes {
        let connect = tarpc::serde_transport::tcp::connect(addr, Json::default).await;
        if let Ok(connect) = connect {
            let client = RaftServiceClient::new(client::Config::default(), connect).spawn();
            clients.push(client);
        }
    }
    while let Some(rpc) = rx.recv().await {
        match rpc {
            OutboundRPCs::AppendEntries(rpc) => {
                for client in clients.iter() {
                    let result = client.append_entries(context::current(), rpc.clone()).await;
                    if let Ok(result) = result {
                        inbox_tx
                            .send(InboundRPCs::AppendEntriesResponse(result))
                            .await;
                    } else {
                        todo!();
                    }
                }
            }
            OutboundRPCs::RequestForVote(rpc) => {
                for client in clients.iter() {
                    let result = client
                        .request_for_vote(context::current(), rpc.clone())
                        .await;
                    if let Ok(result) = result {
                        inbox_tx
                            .send(InboundRPCs::RequestForVoteResponse(result))
                            .await;
                    } else {
                        todo!();
                    }
                }
            }
        }
    }
}
