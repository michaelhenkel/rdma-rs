use std::{collections::HashMap, sync::Arc};

use crate::rdma_server::RdmaServer;
use tokio::sync::RwLock;

pub struct ServerManager{
    pub client: ServerManagerClient,
    rx: Arc<RwLock<tokio::sync::mpsc::Receiver<ServerManagerCommand>>>,
    address: String,
}

impl ServerManager{
    pub fn new(address: String) -> Self{
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let client = ServerManagerClient::new(tx);
        ServerManager{
            client,
            rx: Arc::new(RwLock::new(rx)),
            address
        }
    }

    pub async fn run(self){
        let mut rx = self.rx.write().await;
        let rdma_server = RdmaServer::new();
        let rdma_server_client = rdma_server.client.clone();
        tokio::spawn(async move{
            rdma_server.run().await.unwrap();
        });
        while let Some(server_manager_command) = rx.recv().await{
            let address = self.address.clone();
            match server_manager_command{
                ServerManagerCommand::ConnectionRequest{client_id, qps, tx} => {

                    let mut ports = Vec::new();
                    for _ in 0..qps{
                        let port = portpicker::pick_unused_port().unwrap();
                        ports.push(port as u32);
                    }
                    let address = address.clone();


                    let rdma_server_client = rdma_server_client.clone();
                    let ports_u16 = ports.iter().map(|x| *x as u16).collect();
                    tokio::spawn(async move{
                        //rdma_server.clone().listen(address.clone(), port).await.unwrap();
                        rdma_server_client.clone().connect(address.clone(), ports_u16).await.unwrap();
                    });
                    tx.send(ports).unwrap();
                },
                ServerManagerCommand::Listen{client_id, qp_idx, tx} => {
                    //let rdma_server_client = client_map.get(&(client_id, qp_idx)).unwrap();
                    let mut rdma_server_client = rdma_server_client.clone();
                    tokio::spawn(async move{
                        rdma_server_client.listen().await.unwrap();
                    });
                    tx.send(Ok(())).unwrap();
                }
            }
        }
    }
}
#[derive(Clone)]
pub struct ServerManagerClient{
    tx: tokio::sync::mpsc::Sender<ServerManagerCommand>
}


impl ServerManagerClient{
    pub fn new(tx: tokio::sync::mpsc::Sender<ServerManagerCommand>) -> Self{
        ServerManagerClient{
            tx
        }
    }
    pub async fn request_connection(&mut self, client_id: u32, qps: u32) -> Vec<u32>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(ServerManagerCommand::ConnectionRequest{client_id, qps, tx}).await.unwrap();
        rx.await.unwrap()
    }
    pub async fn listen(&mut self, client_id: u32, qp_idx: u32) -> anyhow::Result<()>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(ServerManagerCommand::Listen{client_id, qp_idx, tx}).await.unwrap();
        rx.await.unwrap()
    }

}

pub enum ServerManagerCommand{
    ConnectionRequest{
        client_id: u32,
        qps: u32,
        tx: tokio::sync::oneshot::Sender<Vec<u32>>
    },
    Listen{
        client_id: u32,
        qp_idx: u32,
        tx: tokio::sync::oneshot::Sender<anyhow::Result<()>>
    }
}