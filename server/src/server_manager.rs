use std::{collections::HashMap, sync::Arc};

use crate::rdma_server::RdmaServerClient;
use tokio::sync::RwLock;

pub struct ServerManager{
    pub client: ServerManagerClient,
    rx: Arc<RwLock<tokio::sync::mpsc::Receiver<ServerManagerCommand>>>,
    address: String,
    rdma_server_client: RdmaServerClient,
}

impl ServerManager{
    pub fn new(address: String, rdma_server_client: RdmaServerClient) -> Self{
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let client = ServerManagerClient::new(tx);
        ServerManager{
            client,
            rx: Arc::new(RwLock::new(rx)),
            address,
            rdma_server_client
        }
    }

    pub async fn run(self){
        let mut rx = self.rx.write().await;
        let mut client_map = HashMap::new();
        while let Some(server_manager_command) = rx.recv().await{
            let address = self.address.clone();
            match server_manager_command{
                ServerManagerCommand::ConnectionRequest{client_id, tx} => {
                    let port = portpicker::pick_unused_port().unwrap();
                    let rdma_server_client_clone_1 = self.rdma_server_client.clone();
                    let rdma_server_client_clone_2 = self.rdma_server_client.clone();
                    tokio::spawn(async move{
                        //rdma_server.clone().listen(address.clone(), port).await.unwrap();
                        rdma_server_client_clone_1.clone().connect(address.clone(), port).await.unwrap();
                    });
                    client_map.insert(client_id, rdma_server_client_clone_2);
                    tx.send(port as u32).unwrap();
                },
                ServerManagerCommand::Listen{client_id, tx} => {
                    let rdma_server_client = client_map.get(&client_id).unwrap();
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
    pub async fn request_connection(&mut self, client_id: u32) -> u32{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(ServerManagerCommand::ConnectionRequest{client_id, tx}).await.unwrap();
        rx.await.unwrap()
    }
    pub async fn listen(&mut self, client_id: u32) -> anyhow::Result<()>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(ServerManagerCommand::Listen{client_id, tx}).await.unwrap();
        rx.await.unwrap()
    }

}

pub enum ServerManagerCommand{
    ConnectionRequest{
        client_id: u32,
        tx: tokio::sync::oneshot::Sender<u32>
    },
    Listen{
        client_id: u32,
        tx: tokio::sync::oneshot::Sender<anyhow::Result<()>>
    }
}