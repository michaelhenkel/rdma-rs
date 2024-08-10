
use std::{ptr::null_mut, sync::Arc};
use common::*;
use rdma_sys::{ibv_access_flags, ibv_reg_mr};
use tokio::sync::RwLock;

use crate::{connection_manager::connection_manager::{MemoryRegionRequest, MemoryRegionResponse, QueuePairRequest, QueuePairResponse}, qp::Qp};



#[derive(Clone)]
pub struct RdmaServer{
    pub client: RdmaServerClient,
    rx: Arc<RwLock<tokio::sync::mpsc::Receiver<RdmaServerCommand>>>,
    address: String,
}

impl RdmaServer{
    pub fn new(address: String) -> RdmaServer{
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let client = RdmaServerClient::new(tx);
        RdmaServer{
            client,
            rx: Arc::new(RwLock::new(rx)),
            address
        }
    }

    pub async fn run(self) -> anyhow::Result<(), CustomError>{
        let mut rx = self.rx.write().await;
        let mut qp_list: Vec<Qp> = Vec::new();
        let src_ip: std::net::Ipv4Addr = self.address.parse().unwrap();
        let (gid, dev_ctx, port, gidx) = get_gid_ctx_port_from_v4_ip(&src_ip)?;
        let pd = create_protection_domain(dev_ctx.ibv_context())?;

        while let Some(rdma_server_command) = rx.recv().await{
            let pd = pd.clone();
            match rdma_server_command{
                RdmaServerCommand::CreateMemoryRegion { memory_region_request, tx } => {
                    let mut data = Data::new(memory_region_request.size as usize);
                    let access_flags = ibv_access_flags::IBV_ACCESS_REMOTE_WRITE | ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_READ;
                    let addr = data.addr();
                    let mr = unsafe { ibv_reg_mr(pd.pd(), addr, memory_region_request.size as usize, access_flags.0 as i32) };
                    if mr == null_mut() {
                        return Err(CustomError::new("Failed to register memory region".to_string(), -1));
                    }
                    let memory_region_response = MemoryRegionResponse{
                        addr: unsafe { (*mr).addr as u64 },
                        rkey: unsafe { (*mr).rkey },
                    };
                    tx.send(memory_region_response).unwrap();

                }
                RdmaServerCommand::CreateQueuePair { qp_request, tx } => {
                    let mut qp = Qp::new(pd, dev_ctx.clone(), gid.clone(), port, gidx);
                    let qp_gid = qp.connect(qp_request)?;
                    qp_list.push(qp);
                    tx.send(qp_gid).unwrap();
                }
            }
        }
        println!("rdma server stopped");
        Ok(())
    }
}

#[derive(Clone)]
pub struct RdmaServerClient{
    tx: tokio::sync::mpsc::Sender<RdmaServerCommand>
}

impl RdmaServerClient{
    pub fn new(tx: tokio::sync::mpsc::Sender<RdmaServerCommand>) -> Self{
        RdmaServerClient{
            tx
        }
    }
    pub async fn create_queue_pair(&mut self, qp_request: QueuePairRequest) -> anyhow::Result<QueuePairResponse>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(RdmaServerCommand::CreateQueuePair{qp_request, tx}).await.unwrap();
        Ok(rx.await.unwrap())
    }
    pub async fn create_memory_region(&mut self, memory_region_request: MemoryRegionRequest) -> anyhow::Result<MemoryRegionResponse>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(RdmaServerCommand::CreateMemoryRegion{memory_region_request, tx}).await.unwrap();
        Ok(rx.await.unwrap())
    }

}

pub enum RdmaServerCommand{
    CreateQueuePair{
        qp_request: QueuePairRequest,
        tx: tokio::sync::oneshot::Sender<QueuePairResponse>
    },
    CreateMemoryRegion{
        memory_region_request: MemoryRegionRequest,
        tx: tokio::sync::oneshot::Sender<MemoryRegionResponse>
    }
}