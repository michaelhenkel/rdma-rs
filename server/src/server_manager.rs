use std::{collections::HashMap, sync::Arc};

use crate::{connection_manager::connection_manager::{MemoryRegionRequest, MemoryRegionResponse, QueuePairRequest, QueuePairResponse}, rdma_server::{RdmaServer, RdmaServerClient}};
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
                ServerManagerCommand::CreateMemoryRegion { memory_region, tx } => {
                    let mut rdma_server_clients = self.rdma_server_clients.write().await;
                    let rdma_server_client = rdma_server_clients.get_mut(&memory_region.client_id).unwrap();
                    let memory_region_response = rdma_server_client.create_memory_region(memory_region).await.unwrap();
                    tx.send(Ok(memory_region_response)).unwrap();
                }
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
                ServerManagerCommand::CreateQueuePair{qp_request, tx} => {
                    let mut rdma_server_clients = self.rdma_server_clients.write().await;
                    let rdma_server_client = rdma_server_clients.get_mut(&qp_request.client_id).unwrap();
                    let qp_response = rdma_server_client.create_queue_pair(qp_request).await.unwrap();
                    tx.send(Ok(qp_response)).unwrap();
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
    // client.exchange_qp_gid(connection_request.client_id, connection_request.qpn, connection_request.gid_subnet_id, connection_request.gid_interface_id).await.unwrap();
    pub async fn create_queue_pair(&mut self, qp_request: QueuePairRequest) -> anyhow::Result<QueuePairResponse>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(ServerManagerCommand::CreateQueuePair{qp_request, tx}).await.unwrap();
        rx.await.unwrap()
    }
    pub async fn create_memory_region(&mut self, memory_region: MemoryRegionRequest) -> anyhow::Result<MemoryRegionResponse>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(ServerManagerCommand::CreateMemoryRegion{memory_region, tx}).await.unwrap();
        rx.await.unwrap()
    }
    

}

pub enum ServerManagerCommand{
    CreateRdmaServer{
        client_id: u32,
        tx: tokio::sync::oneshot::Sender<anyhow::Result<()>>
    },
    CreateQueuePair{
        qp_request: QueuePairRequest,
        tx: tokio::sync::oneshot::Sender<anyhow::Result<QueuePairResponse>>
    },
    CreateMemoryRegion{
        memory_region: MemoryRegionRequest,
        tx: tokio::sync::oneshot::Sender<anyhow::Result<MemoryRegionResponse>>
    }
}