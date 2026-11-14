use std::net::Ipv4Addr;
use std::net::SocketAddr;

use crate::log::LogEntries;
use crate::net;
use crate::raft;
use crate::server;
use crate::server::InboundRPCs;
use tokio::sync::mpsc;

async fn new_node(
    id: crate::raft::NodeId,
    nodes: Vec<(raft::NodeId, SocketAddr)>,
) -> (
    server::RequestHandler,
    net::Net,
    mpsc::Receiver<server::InboundRPCs>,
) {
    let (inbox_tx, inbox_rs) = mpsc::channel(8);

    let inbound = server::RequestHandler::new(inbox_tx.clone());
    inbound
        .start_listen(
            nodes
                .iter()
                .find_map(
                    |&(node_id, addr)| {
                        if node_id == id { Some(addr) } else { None }
                    },
                )
                .unwrap(),
        )
        .await
        .unwrap();

    let outbound = net::Net::new(id, nodes.clone(), inbox_tx.clone());

    (inbound, outbound, inbox_rs)
}

#[tokio::test]
async fn test_legal_transport() {
    let nodes = vec![
        (0, SocketAddr::from((Ipv4Addr::LOCALHOST, 7000))),
        (1, SocketAddr::from((Ipv4Addr::LOCALHOST, 7001))),
        (2, SocketAddr::from((Ipv4Addr::LOCALHOST, 7002))),
    ];
    let mut node0 = new_node(0, nodes.clone()).await;
    let mut node1 = new_node(1, nodes.clone()).await;
    let mut node2 = new_node(2, nodes.clone()).await;

    let rpc = raft::AppendEntriesRPC {
        term: 2,
        leader_id: 0,
        prev_log_index: 0,
        prev_log_term: 1,
        entries: LogEntries::new(),
        leader_commit: 1,
    };
    let out_rpc = net::OutboundRPCs::AppendEntries(rpc.clone());

    node0.1.send_rpc(1, out_rpc.clone());
    node0.1.send_rpc(2, out_rpc.clone());

    let rpc_node1_received = node1.2.recv().await.unwrap();
    match rpc_node1_received {
        InboundRPCs::AppendEntries(r, tx) => {
            assert_eq!(r, rpc.clone());
            assert!(tx.send((r.term, true)).is_ok());
        }
        _ => unreachable!(),
    }
    let rpc_node2_received = node2.2.recv().await.unwrap();
    match rpc_node2_received {
        InboundRPCs::AppendEntries(r, tx) => {
            assert_eq!(r, rpc.clone());
            assert!(tx.send((r.term, true)).is_ok());
        }
        _ => unreachable!(),
    }

    // TODO: sequential dependency
    let reply_node0_received_from_node1 = node0.2.recv().await.unwrap();
    let reply_node0_received_from_node2 = node0.2.recv().await.unwrap();
    match reply_node0_received_from_node1 {
        InboundRPCs::AppendEntriesResponse((term, success), id) => {
            assert_eq!(term, 2);
            assert!(success);
            assert_eq!(id, 1);
        }
        _ => unreachable!(),
    }
    match reply_node0_received_from_node2 {
        InboundRPCs::AppendEntriesResponse((term, success), id) => {
            assert_eq!(term, 2);
            assert!(success);
            assert_eq!(id, 2);
        }
        _ => unreachable!(),
    }
}

// TODO: sad path
