use std::{collections::HashMap, sync::Arc};

use crate::rdma_server::{RdmaServer, RdmaServerClient};
use tokio::sync::RwLock;

pub struct ServerManager{
    pub client: ServerManagerClient,
    rx: Arc<RwLock<tokio::sync::mpsc::Receiver<ServerManagerCommand>>>,
    address: String,
    rdma_server_clients: Arc<RwLock<HashMap<u32, RdmaServerClient>>>,
}

impl ServerManager{
    pub fn new(address: String) -> Self{
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let client = ServerManagerClient::new(tx);
        ServerManager{
            client,
            rx: Arc::new(RwLock::new(rx)),
            address,
            rdma_server_clients: Arc::new(RwLock::new(HashMap::new()))
        }
    }

    pub async fn run(&mut self){
        let mut rx = self.rx.write().await;

        while let Some(server_manager_command) = rx.recv().await{
            //let address = self.address.clone();
            match server_manager_command{
                ServerManagerCommand::CreateRdmaServer{client_id, tx} => {
                    let rdma_server = RdmaServer::new(self.address.clone());
                    let rdma_server_client = rdma_server.client.clone();
                    tokio::spawn(async move{
                        rdma_server.run().await.unwrap();
                    });
                    let mut rdma_server_clients = self.rdma_server_clients.write().await;
                    rdma_server_clients.insert(client_id, rdma_server_client);
                    tx.send(Ok(())).unwrap();
                },
                ServerManagerCommand::ConnectQp { client_id, port, tx } => {
                    let mut rdma_server_clients = self.rdma_server_clients.write().await;
                    let rdma_server_client = rdma_server_clients.get_mut(&client_id).unwrap();
                    rdma_server_client.connect_qp(port).await.unwrap();
                    tx.send(Ok(())).unwrap();
                }
                ServerManagerCommand::Listen{client_id, port, tx} => {
                    let mut rdma_server_clients = self.rdma_server_clients.write().await;
                    let mut rdma_server_client = rdma_server_clients.get_mut(&client_id).unwrap().clone();
                    tokio::spawn(async move{
                        rdma_server_client.listen(port).await.unwrap();
                    });
                    tx.send(Ok(())).unwrap();
                },
                ServerManagerCommand::CreateQp{client_id, tx} => {
                    let mut rdma_server_clients = self.rdma_server_clients.write().await;
                    let rdma_server_client = rdma_server_clients.get_mut(&client_id).unwrap();
                    let qp_idx = rdma_server_client.create_qp().await.unwrap();
                    tx.send(Ok(qp_idx)).unwrap();
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
    pub async fn create_rdma_server(&mut self, client_id: u32) -> anyhow::Result<()>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(ServerManagerCommand::CreateRdmaServer{client_id, tx}).await.unwrap();
        rx.await.unwrap()
    }
    pub async fn listen(&mut self, client_id: u32, port: u16) -> anyhow::Result<()>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(ServerManagerCommand::Listen{client_id, port, tx}).await.unwrap();
        rx.await.unwrap()
    }
    pub async fn create_qp(&mut self, client_id: u32) -> anyhow::Result<u16>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(ServerManagerCommand::CreateQp{client_id, tx}).await.unwrap();
        rx.await.unwrap()
    }
    pub async fn connect_qp(&mut self, client_id: u32, port: u16) -> anyhow::Result<()>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(ServerManagerCommand::ConnectQp{client_id, port, tx}).await.unwrap();
        rx.await.unwrap()
    }

}

pub enum ServerManagerCommand{
    CreateRdmaServer{
        client_id: u32,
        tx: tokio::sync::oneshot::Sender<anyhow::Result<()>>
    },
    Listen{
        client_id: u32,
        port: u16,
        tx: tokio::sync::oneshot::Sender<anyhow::Result<()>>
    },
    CreateQp{
        client_id: u32,
        tx: tokio::sync::oneshot::Sender<anyhow::Result<u16>>
    },
    ConnectQp{
        client_id: u32,
        port: u16,
        tx: tokio::sync::oneshot::Sender<anyhow::Result<()>>
    }
}