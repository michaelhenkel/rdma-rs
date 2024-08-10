use std::ptr::null_mut;
use common::*;
use log::info;
use rdma_sys::{ibv_gid, ibv_qp};

use crate::connection_manager::connection_manager::{QueuePairRequest, QueuePairResponse};

pub struct Qp{
    pd: ProtectionDomain,
    pub qp: QueuePair,
    ctx: IbvContext,
    gid: IbvGid,
    port: u8,
    gidx: i32,
}

impl Qp{
    pub fn new(pd: ProtectionDomain, ctx: IbvContext, gid: IbvGid, port: u8, gidx: i32) -> Self {
        Qp{
            pd,
            qp: QueuePair(null_mut()),
            ctx,
            gid,
            port,
            gidx,
        }
    }
    pub fn connect(&mut self, qp_request: QueuePairRequest) -> anyhow::Result<QueuePairResponse, CustomError>{
        let event_channel = create_event_channel(self.ctx.ibv_context())?;
        let cq = create_create_completion_queue(self.ctx.ibv_context(), event_channel.event_channel());
        if cq == null_mut() {
            return Err(CustomError::new("Failed to create completion queue".to_string(), -1));
        }
        let qp = create_queue_pair(self.pd.pd(), cq);
        if qp == null_mut() {
            return Err(CustomError::new("Failed to create queue pair".to_string(), -1));
        }
        let qp: Box<ibv_qp> = unsafe { Box::from_raw(qp) };
        let qp = Box::into_raw(qp);

        let ret = set_qp_init_state(qp, self.port);
        if ret != 0 {
            return Err(CustomError::new("Failed to set qp init state".to_string(), ret));
        }
        let subnet_prefix_bytes = qp_request.gid_subnet_id.to_be_bytes();
        let interface_id_bytes = qp_request.gid_interface_id.to_be_bytes();
        let subnet_prefix_bytes = subnet_prefix_bytes.iter().rev().cloned().collect::<Vec<u8>>();
        let interface_id_bytes = interface_id_bytes.iter().rev().cloned().collect::<Vec<u8>>();
        let mut raw = [0u8; 16];
        raw[..8].copy_from_slice(&subnet_prefix_bytes);
        raw[8..].copy_from_slice(&interface_id_bytes);
        let remote_gid = ibv_gid{
            raw,
        };

        let my_psn = gen_psn();
        connect_qp(qp, remote_gid, qp_request.qpn, qp_request.psn, my_psn, self.gidx)?;
        info!("Connected to remote qp: {}", gid_to_ipv6_string(remote_gid).unwrap());
        let local_qpn = unsafe { (*qp).qp_num };

        let qp_gid = QueuePairResponse { 
            qpn: local_qpn,
            gid_subnet_id: unsafe { (*self.gid.ibv_gid()).global.subnet_prefix },
            gid_interface_id: unsafe { (*self.gid.ibv_gid()).global.interface_id },
            lid: 0,
            psn: my_psn,
        };
        self.qp = QueuePair(qp);
        Ok(qp_gid)
    }
}