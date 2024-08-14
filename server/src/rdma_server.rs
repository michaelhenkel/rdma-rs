
use std::{ptr::null_mut, sync::Arc};
use common::*;
use rdma_sys::{ibv_access_flags, ibv_gid, ibv_reg_mr};
use tokio::sync::RwLock;

use crate::{connection_manager::connection_manager::{MemoryRegionRequest, MemoryRegionResponse, QueuePairRequest, QueuePairResponse}, qp::Qp};

pub struct RdmaServer{
    pub client: RdmaServerClient,
    rx: Arc<RwLock<tokio::sync::mpsc::Receiver<RdmaServerCommand>>>,
    gid_list: Vec<GidEntry>,
    single_gid: Option<GidEntry>,
    dev_ctx: IbvContext,
}

impl RdmaServer{
    pub async fn new(address: String, device_name: Option<String>, family: Family, mode: Mode, qpns: u32) -> anyhow::Result<RdmaServer, CustomError>{
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let client = RdmaServerClient::new(tx);

        let gid_table = generate_gid_table(device_name.clone())?;
        let dev_ctx = gid_table.context.clone();

        let ip_opt = match mode{
            Mode::SingleIp => Some(address.clone()),
            Mode::MultiIp => None,
        };

        let (gid_list, single_gid) = get_single_or_multi_gid(gid_table, family.clone(), qpns as usize, ip_opt).await?;


        Ok(RdmaServer{
            client,
            rx: Arc::new(RwLock::new(rx)),
            gid_list,
            single_gid,
            dev_ctx,
        })
    }

    pub async fn run(self) -> anyhow::Result<(), CustomError>{
        let mut rx = self.rx.write().await;
        let mut qp_list: Vec<Qp> = Vec::new();
        //let src_ip: std::net::Ipv4Addr = self.address.parse().unwrap();
        //let (gid, dev_ctx, port, gidx) = get_gid_ctx_port_from_v4_ip(&src_ip)?;
        let dev_ctx = self.dev_ctx.clone();
        let pd = create_protection_domain(dev_ctx.ibv_context())?;
        let gid_list = self.gid_list.clone();
        let single_gid = self.single_gid.clone();
        let mut qp_idx = 0;

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
                    let gid_entry = if let Some(gid_entry) = single_gid.clone(){
                        gid_entry
                    } else {
                        if qp_idx >= gid_list.len(){
                            return Err(CustomError::new("Not enough GIDs".to_string(), -1));
                        }
                        let gid_entry = gid_list[qp_idx].clone();
                        gid_entry  
                    };
                    let gid = gid_entry.gid;
                    let port = gid_entry.port;
                    let gidx = gid_entry.gidx;
                    let local_gid: ibv_gid = unsafe { *gid.ibv_gid() };
                    let local_gid_v6 = gid_to_ipv6_string(local_gid);
                    let subnet_prefix_bytes = qp_request.gid_subnet_id.to_be_bytes();
                    let interface_id_bytes = qp_request.gid_interface_id.to_be_bytes();
                    let subnet_prefix_bytes = subnet_prefix_bytes.iter().rev().cloned().collect::<Vec<u8>>();
                    let interface_id_bytes = interface_id_bytes.iter().rev().cloned().collect::<Vec<u8>>();
                    let mut raw = [0u8; 16];
                    raw[..8].copy_from_slice(&subnet_prefix_bytes);
                    raw[8..].copy_from_slice(&interface_id_bytes);
                    let remote_gid_v6 = gid_to_ipv6_string(ibv_gid{raw});
                    println!("Local GID: {}, Remote GID {}", local_gid_v6.unwrap(), remote_gid_v6.unwrap());
                    // combined qp_request.subnet_id and qp_request.interface_id to a 128bit ipv6 address

                    let mut qp = Qp::new(pd, dev_ctx.clone(), gid.clone(), port, gidx);
                    let qp_gid = qp.connect(qp_request)?;
                    qp_list.push(qp);
                    qp_idx += 1;
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