use std::ptr::{self, null_mut};
use common::*;
use log::info;
use rdma_sys::*;

use crate::{connection_manager::connection_manager::{MemoryRegionRequest, MemoryRegionResponse, QueuePairRequest}, grpc_client::GrpcClient};

const BATCH_SIZE: u64 = 2000;

pub struct RdmaClient{
    qps_cqs: Vec<(QueuePair, CompletionQueue, EventChannel)>,
    pd: ProtectionDomain,
    grpc_client: GrpcClient,
}

impl RdmaClient{
    pub async fn new(ip: String, port: u16, num_qp: usize) -> anyhow::Result<RdmaClient, CustomError>{
        let grpc_address = format!("http://{}:{}",ip,port);
        let grpc_client = GrpcClient::new(grpc_address,0 );
        grpc_client.create_rdma_server().await.unwrap();
        let src_ip = route_lookup(ip.clone()).await?;
        let (gid, dev_ctx, port, gidx) = get_gid_ctx_port_from_v4_ip(&src_ip)?;
        let gid_subnet_id = unsafe { (*gid.ibv_gid()).global.subnet_prefix };
        let gid_interface_id = unsafe { (*gid.ibv_gid()).global.interface_id };
        if unsafe { (*gid.ibv_gid()).global.interface_id } == 0 {
            return Err(CustomError::new("Failed to get gid 0".to_string(), -1).into());
        }
        info!("GID LOCAL Interface_id: {:?}", unsafe { (*gid.ibv_gid()).global.subnet_prefix });

        if gid_interface_id == 0 {
            return Err(CustomError::new("Failed to get gid 1".to_string(), -1).into());
        }
        let pd = create_protection_domain(dev_ctx.ibv_context())?;
        let mut qp_list: Vec<(QueuePair, CompletionQueue, EventChannel)> = Vec::new();
        for _ in 0..num_qp{
            let event_channel = create_event_channel(dev_ctx.ibv_context())?;
            let cq = create_create_completion_queue(dev_ctx.ibv_context(), event_channel.event_channel());
            if cq == null_mut() {
                return Err(CustomError::new("Failed to create completion queue".to_string(), -1).into());
            }

            let qp = create_queue_pair(pd.pd(), cq);
            if qp == null_mut() {
                return Err(CustomError::new("Failed to create queue pair".to_string(), -1).into());
            }
            let ret = set_qp_init_state(qp, port);
            if ret != 0 {
                return Err(CustomError::new("Failed to set qp init state".to_string(), ret).into());
            }

            let psn = gen_psn();

            let qpn = unsafe { (*qp).qp_num };
            let my_qp_gid = QueuePairRequest{
                client_id: 0,
                qpn,
                gid_subnet_id,
                gid_interface_id,
                lid: 0,
                psn,
            };
            let remote_qp_gid = grpc_client.create_queue_pair(my_qp_gid).await.unwrap();
            let subnet_prefix_bytes = remote_qp_gid.gid_subnet_id.to_be_bytes();
            let interface_id_bytes = remote_qp_gid.gid_interface_id.to_be_bytes();
            let subnet_prefix_bytes = subnet_prefix_bytes.iter().rev().cloned().collect::<Vec<u8>>();
            let interface_id_bytes = interface_id_bytes.iter().rev().cloned().collect::<Vec<u8>>();
            let mut raw = [0u8; 16];
        
            raw[..8].copy_from_slice(&subnet_prefix_bytes);
            raw[8..].copy_from_slice(&interface_id_bytes);
        
        
            let remote_gid = ibv_gid{
                raw,
            };
            connect_qp(qp, remote_gid, remote_qp_gid.qpn, remote_qp_gid.psn, psn, gidx)?;
            qp_list.push((QueuePair(qp), CompletionQueue(cq), event_channel));

        }

        Ok(RdmaClient{
            qps_cqs: qp_list,
            pd,
            grpc_client,
        })
    }

    pub async fn register_memory_region(&self, size: u64) -> anyhow::Result<(MemoryRegion, MemoryRegionResponse), CustomError>{
        let mut data = Data::new(size as usize);
        let access_flags = ibv_access_flags::IBV_ACCESS_REMOTE_WRITE | ibv_access_flags::IBV_ACCESS_LOCAL_WRITE | ibv_access_flags::IBV_ACCESS_REMOTE_READ;
        let addr = data.addr();
        let mr = unsafe { ibv_reg_mr(self.pd.pd(), addr, size as usize, access_flags.0 as i32) };
        if mr == null_mut() {
            return Err(CustomError::new("Failed to register memory region".to_string(), -1));
        }
        let memory_region_request = MemoryRegionRequest{
            client_id: 0,
            size,
        };
        let memory_region_response = self.grpc_client.create_memory_region(memory_region_request).await.unwrap();
        Ok((MemoryRegion(mr), memory_region_response))
    }

    pub async fn write(&self, local_mr: MemoryRegion, remote_mr: MemoryRegionResponse, iterations: usize, message_size: u64, _delay: u64) -> anyhow::Result<(), CustomError>{
        let num_qps = self.qps_cqs.len();
        let volume = unsafe { (*local_mr.mr()).length };
        let mut queues = distribute_data(
            volume as u64,
            num_qps as u64,
            message_size,
            BATCH_SIZE as u64,
            1,
            unsafe { (*local_mr.mr()).addr as u64},
            remote_mr.addr,
            unsafe { (*local_mr.mr()).lkey },
            remote_mr.rkey,
        );
        let mut jh_list = Vec::new();
        let mut qp_idx = 0;
        let now = std::time::Instant::now();
        let total_volume = volume as u64 * iterations as u64;
        let volume_per_qp = total_volume / num_qps as u64;
        while let Some(qp) = queues.pop(){
            let mut volume_per_qp = volume_per_qp.clone();
            //let mut volume_per_qp_2 = volume_per_qp.clone();
            let delay = _delay * qp_idx as u64;
            let qps_cqs = self.qps_cqs.clone();
            //let mut total_len = 0;
            //let mut qp_message_size = qp.qp_message_size * iterations as u64;
            //let mut qp_message_size_2 = qp.qp_message_size * iterations as u64;
            println!("qp_idx: {} has {} message blocks, qp msg_size: {}", qp_idx, qp.message_blocks.len(), qp.qp_message_size);
            //let delay = delay * qp_idx as u64;
            let jh = tokio::spawn(async move{
                println!("qp {} started with delay {}", qp_idx,delay);
                if delay > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
                }
                
                
                /*
                while volume_per_qp_2 > 0 {
                    for (msg_block_idx, msg_block) in qp.message_blocks.iter().enumerate(){
                    
                        println!("Message Block: {} messages {}", msg_block_idx, msg_block.message_count);
                        let l = print_wr_ids(msg_block.wr.send_wr());
                        total_len += l;
                        volume_per_qp_2 -= msg_block.message_block_size;
                    }
                }
                println!("Total length: {}", total_len);
                */
                let queue_pair = &qps_cqs[qp_idx].0;
                let cq = &qps_cqs[qp_idx].1;
                let event_channel = &qps_cqs[qp_idx].2;
                let mut bad_wr = null_mut();
                let mut required_completions = 0;
                let mut sent_messages = 0;
                while volume_per_qp > 0 {
                    for (_msg_block_idx, msg_block) in qp.message_blocks.iter().enumerate(){
                        let ret = unsafe { ibv_post_send(queue_pair.qp(), msg_block.wr.send_wr(), &mut bad_wr) };
                        if ret != 0 {
                            println!("ibv_post_send failed");
                        }
                        required_completions += 1;
                        sent_messages += msg_block.message_count;
                        volume_per_qp -= msg_block.message_block_size;
                        if sent_messages as u64 >= BATCH_SIZE || required_completions as u64 >= BATCH_SIZE {
                            let _ret = unsafe { send_complete(cq.cq(), event_channel.event_channel(), required_completions, ibv_wc_opcode::IBV_WC_RDMA_WRITE).unwrap() };
                            //println!("0 completed {} messages, sent_messages: {}, required_completions: {}", _ret, sent_messages, required_completions);
                            required_completions -= _ret as usize;
                            sent_messages = 0;

                        }
                        //let _ret = unsafe { send_complete(cq.cq(), event_channel.event_channel(), 1, ibv_wc_opcode::IBV_WC_RDMA_WRITE).unwrap() };
                    }
                }
                if required_completions > 0 {
                    let _ret = unsafe { send_complete(cq.cq(), event_channel.event_channel(), required_completions, ibv_wc_opcode::IBV_WC_RDMA_WRITE).unwrap() };
                    println!("1 completed {} messages", _ret);
                }

            });
            jh_list.push(jh);
            qp_idx += 1;
        }
        futures::future::join_all(jh_list).await;
        let elapsed = now.elapsed();
        let total_volume: u64 = volume as u64 * iterations as u64 ;
        let message_size_byte = byte_unit::Byte::from_u64(total_volume);
        let message_size_string = format!("{message_size_byte:#}");
        let gbit = (total_volume * 8) as f64 / 1_000_000_000.0;
        let gbps = gbit / elapsed.as_secs_f64();
        println!("total: wrote {} in {:.2}s: {:.2} Gbps",message_size_string, elapsed.as_secs_f64(), gbps);

        Ok(())
    }
    
}

pub fn print_wr_ids(start: *mut ibv_send_wr) -> u64{
    let mut current = start;
    let mut total_len = 0;
    while !unsafe { (*current).next.is_null() } {
        let sge = & unsafe { (*current).sg_list };
        let sge_len = unsafe { (**sge).length as u64 };
        total_len += sge_len;
        let sge_addr = unsafe { (**sge).addr };
        let remote_addr = unsafe { (*current).wr.rdma.remote_addr };
        let wr_send_flag = unsafe { (*current).send_flags };
        println!("wr_id: {}, sge_addr: {}, sge_len: {}, remote_addr: {}, send_flag {}", unsafe { (*current).wr_id }, sge_addr, sge_len, remote_addr, wr_send_flag);
        current = unsafe { (*current).next };

    }
    let sge = &unsafe { (*current).sg_list };
    let sge_len = unsafe { (**sge).length as u64 };
    total_len += sge_len;
    let sge_addr = unsafe { (**sge).addr };
    let remote_addr = unsafe { (*current).wr.rdma.remote_addr };
    let wr_send_flag = unsafe { (*current).send_flags };
    println!("wr_id: {}, sge_addr: {}, sge_len: {}, remote_addr: {}, send_flag {}", unsafe { (*current).wr_id }, sge_addr, sge_len, remote_addr, wr_send_flag);
    total_len
}

struct Qp {
    message_blocks: Vec<MessageBlock>,
    qp_message_size: u64,
}

impl Drop for Qp {
    fn drop(&mut self) {
        for message_block in &self.message_blocks {
            unsafe {
                let _ = Box::from_raw((*message_block.wr.send_wr()).sg_list);
            }
            unsafe {
                let _ = Box::from_raw(message_block.wr.send_wr());
            }
        }
    }
}

struct MessageBlock {
    message_count: usize,
    wr: IbvSendWr,
    message_block_size: u64,
}

fn distribute_data(
    volume: u64,
    queue_pairs: u64,
    max_message_size: u64,
    max_msg_per_block: u64,
    iterator_factor: u64,
    local_mr_start_addr: u64,
    remote_mr_start_addr: u64,
    lkey: u32,
    rkey: u32,
) -> Vec<Qp> {
    // Calculate the base size for each queue pair
    let base_size = volume / queue_pairs;
    let remainder = volume % queue_pairs;

    // Initialize queues
    let mut queues = Vec::new();
    for _ in 0..queue_pairs {
        queues.push(Qp {
            message_blocks: Vec::new(),
            qp_message_size: 0,
        });
    }
    for factor in 0..iterator_factor {
        let mut current_offset = factor * volume;

        for i in 0..queue_pairs {
            let mut size = base_size;
            if i < remainder {
                size += 1; // Distribute the remainder
            }

            let mut current_block = MessageBlock {
                message_count: 0,
                wr: IbvSendWr(ptr::null_mut()),
                message_block_size: 0,
            };
            let mut current_block_size = 0;
            let mut last_wr: *mut ibv_send_wr = ptr::null_mut();

            while size > 0 {
                let message_size = if size > max_message_size { max_message_size } else { size };
                let sge = ibv_sge{
                    addr: (local_mr_start_addr + (current_offset % volume)) as u64,
                    length: message_size as u32,
                    lkey,
                };
                current_block.message_block_size += message_size;
                let sge = Box::new(sge);
                let sge_ptr: *mut ibv_sge = Box::into_raw(sge);
                let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
                wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
                wr.sg_list = sge_ptr;
                wr.num_sge = 1;
                wr.wr.rdma.remote_addr = remote_mr_start_addr + (current_offset % volume);
                wr.wr.rdma.rkey = rkey;
                if size == message_size {
                    wr.send_flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
                } else {
                    wr.send_flags = 0;
                }
                let wr = Box::new(wr);
                let wr_ptr: *mut ibv_send_wr = Box::into_raw(wr);
                // If there is a previous WR, set its next to point to the current WR
                if !last_wr.is_null() {
                    unsafe {
                        (*last_wr).next = wr_ptr; // Link the previous WR to the current WR
                    }
                } else {
                    current_block.wr.0 = wr_ptr;
                }
                last_wr = wr_ptr;

                current_block.message_count += 1;
                current_offset = (current_offset + message_size) % volume;
                current_block_size += 1;
                size -= message_size;

                if current_block_size >= max_msg_per_block {
                    queues[i as usize].message_blocks.push(current_block);
                    current_block = MessageBlock {
                        message_count: 0,
                        wr: IbvSendWr(ptr::null_mut()),
                        message_block_size: 0,
                    };
                    last_wr = ptr::null_mut();
                    current_block_size = 0;
                }
            }
            
            if current_block.message_count > 0 {
                queues[i as usize].qp_message_size += current_block.message_block_size;
                queues[i as usize].message_blocks.push(current_block);
            }
        }
    }

    queues
}