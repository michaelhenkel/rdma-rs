use std::{collections::HashMap, os::raw::c_void, ptr::null_mut};
use common::*;
use rdma_sys::*;
use tokio::time;


pub struct RdmaClient{
    id: Vec<Id>,
}

impl RdmaClient{
    pub fn new() -> RdmaClient{
        RdmaClient{
            id: Vec::new(),
        }
    }

    pub fn connect(&mut self, ip: &str, port: &str, pd: &mut Option<ProtectionDomain>) -> anyhow::Result<(), CustomError>{
        let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
        let mut res: *mut rdma_addrinfo = null_mut();
    
        hints.ai_port_space = rdma_port_space::RDMA_PS_TCP as i32;
        let ret =
            unsafe { rdma_getaddrinfo(ip.as_ptr().cast(), port.as_ptr().cast(), &hints, &mut res) };
    
        if ret != 0 {
            return Err(CustomError::new("rdma_getaddrinfo".to_string(), ret).into());
        }
    
        let mut attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        let mut id: *mut rdma_cm_id = null_mut();
        attr.cap.max_send_wr = 4096;
        attr.cap.max_recv_wr = 4096;
        attr.cap.max_send_sge = 1;
        attr.cap.max_recv_sge = 1;
        attr.cap.max_inline_data = 64;
        attr.qp_context = id.cast();
        attr.sq_sig_all = 0;

        if pd.is_none(){
            *pd = Some(ProtectionDomain(null_mut()));
        }

        let ret = unsafe { rdma_create_ep(&mut id, res, pd.as_ref().unwrap().pd(), &mut attr) };
        if ret != 0 {
            unsafe { rdma_freeaddrinfo(res); }
            return Err(CustomError::new("rdma_create_ep".to_string(), ret).into());
        }

        let ret = unsafe { rdma_connect(id, null_mut()) };
        if ret != 0 {
            unsafe { rdma_disconnect(id); }
            return Err(CustomError::new("rdma_connect".to_string(), ret).into());
        }
        self.id.push(Id(id));
        Ok(())
    }

    pub fn disconnect(&self) -> anyhow::Result<(), CustomError>{
        println!("Disconnecting");
        for id in &self.id{
            let mut metadata_request = MetaData::default();
            metadata_request.set_request_type(MetaDataRequestTypes::Disconnect);
            let mr_addr = metadata_request.create_and_register_mr(id, Operation::SendRecv)?;
            metadata_request.rdma_send(id, &mr_addr)?;
        }
        Ok(())
    }

    pub async fn write(&self, message_size: u64, volume: u64, iterations: usize) -> anyhow::Result<(), CustomError> {

        let messages = volume/message_size;
        let remaining_bytes = volume % message_size;

        let qps_to_use = if messages > self.id.len() as u64 { self.id.len() as u64 } else { messages };
        let messages_per_qp = messages / qps_to_use;
        let mut data_list = Vec::with_capacity(qps_to_use as usize);
        let mut metadata_mr_map = HashMap::new();
        let mut data = Data::new(volume as usize);
        let data_addr = data.addr();
        let data_len = data.len();
        let mut rkey = None;
        let mut remote_address = None;
        for qp_idx in 0..qps_to_use{
            let id = &self.id[qp_idx as usize];
            if id.id().is_null(){
                return Err(CustomError::new("id is null".to_string(), -1).into());
            }
            let mut metadata_request = MetaData::default();
            metadata_request.set_request_type(MetaDataRequestTypes::WriteRequest);
            if let Some(rkey) = rkey{
                metadata_request.set_rkey(rkey);
            }
            if let Some(remote_address) = remote_address{
                metadata_request.set_remote_address(remote_address);
            }
            metadata_request.set_message_size(volume);
            let metadata_mr_addr = metadata_request.create_and_register_mr(&id, Operation::SendRecv)?;
            metadata_request.rdma_send(&id, &metadata_mr_addr)?;
            metadata_request.rdma_recv(&id, &metadata_mr_addr)?;
            if rkey.is_none(){
                rkey = Some(metadata_request.rkey());
            }
            if remote_address.is_none(){
                remote_address = Some(metadata_request.remote_address());
            }
            let mr = unsafe { rdma_reg_write(id.id(), data_addr, data_len) };
            if mr.is_null(){
                return Err(CustomError::new("rdma_reg_write".to_string(), -1).into());
            }
            let mr_addr = MrAddr{mr, addr: data_addr};
            data_list.push((mr_addr, id.clone(), metadata_request));
            metadata_mr_map.insert(qp_idx, metadata_mr_addr);
        }

        println!("qps to use: {}, messages per qp: {}", qps_to_use, messages_per_qp);

        let mut sge_list = Vec::new();
        let mut offset = 0;
        for qp_idx in 0..qps_to_use{
            let (mr_addr, _id, _metadata_request) = data_list.get_mut(qp_idx as usize).unwrap();
            for _ in 0..messages_per_qp{
                let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
                let addr = unsafe { (*mr_addr.mr).addr };
                sge.addr = addr.wrapping_add(offset as usize) as u64;
                sge.length = message_size as u32;
                sge.lkey = unsafe { (*mr_addr.mr).lkey };
                sge_list.push(sge);
                offset += message_size;
            }
        }

        let mut wr_idx = 0;
        let mut wr_list = Vec::new();
        let mut offset = 0;
        for qp_idx in 0..qps_to_use{
            let (_mr_addr, id, metadata_request) = data_list.get_mut(qp_idx as usize).unwrap();
            for _ in 0..messages_per_qp{
                let sge = sge_list.get_mut(wr_idx).unwrap();
                let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
                wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
                wr.send_flags =  ibv_send_flags::IBV_SEND_SIGNALED.0;
                wr.sg_list = &mut *sge;
                wr.num_sge = 1;
                let remote_addr = metadata_request.remote_address();
                let remote_addr = remote_addr.wrapping_add(offset);
                wr.wr.rdma.remote_addr = remote_addr;
                wr.wr.rdma.rkey = metadata_request.rkey();
                wr_list.push((id.clone(), wr));
                offset += message_size;
                wr_idx += 1;
            }
        }

    

        for (id, mut wr) in wr_list{
            let qp = unsafe { (*id.id()).qp };
            let mut bad_wr = null_mut();
            let ret = unsafe { ibv_post_send(qp, &mut wr, &mut bad_wr) };
            if ret != 0 {
                return Err(CustomError::new("ibv_post_send".to_string(), ret).into());
            }
            unsafe { send_complete(id, 1, ibv_wc_opcode::IBV_WC_RDMA_WRITE)? };
        }
    
        


        /*

        let mut qp_wr_list = Vec::new();

        for qp_idx in 0..qps_to_use{
            let mut data_list = data_list.clone();
            let data_addr = data_addr.clone();
            let (mr_addr, id, metadata_request) = data_list.remove(qp_idx as usize);
            let mut sge_offset = qp_idx * messages_per_qp * message_size;
            let mut wr_offset = qp_idx * messages_per_qp * message_size;
            let rkey = metadata_request.rkey();
            let remote_address = metadata_request.remote_address(); 
            let mr = Mr(mr_addr.mr);
            let mut flags = 0;
            let mut sge_list = vec![unsafe { std::mem::zeroed::<ibv_sge>() }; messages_per_qp as usize];
            println!("prepare sge list");
            for msg in 0..messages_per_qp{
                println!("creating sge {}, offset: {}",msg, sge_offset);
                let addr = unsafe { (*mr.mr()).addr };
                let addr = addr.wrapping_add(sge_offset as usize);
                sge_list[msg as usize].addr = addr as u64;
                sge_list[msg as usize].length = message_size as u32;
                sge_list[msg as usize].lkey = unsafe { (*mr.mr()).lkey };
                sge_offset += message_size;
            }
            let mut wr_list = Vec::new();
            println!("prepare wr list");
            for msg in 0..messages_per_qp{
                println!("creating wr {}, offset {}",msg, wr_offset);
                let mut send_wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
                send_wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
                if msg == messages_per_qp - 1 || (msg as usize) + 1 % 2000 == 0{
                    flags = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0 | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0 | ibv_send_flags::IBV_SEND_SIGNALED.0;
                }
                send_wr.send_flags = flags;
                let mut _wr_t = unsafe { std::mem::zeroed::<wr_t>() };
                let mut _rdma_t = unsafe { std::mem::zeroed::<rdma_t>() };
                _rdma_t.remote_addr = remote_address.wrapping_add(wr_offset);
                _rdma_t.rkey = rkey;
                _wr_t.rdma = _rdma_t;
                send_wr.wr = _wr_t;
                //let mut send_sge = sge_list[msg as usize];
                let sge_list_ptr = sge_list.as_mut_ptr();
                send_wr.sg_list = sge_list_ptr;
                send_wr.num_sge = 1;
                wr_list.push(send_wr);
                wr_offset += message_size;
            }
            
            for i in 0..wr_list.len() - 1 {
                let next_ptr: *mut ibv_send_wr = &mut wr_list[i + 1] as *mut _;
                wr_list[i].next = next_ptr;
            }
            let wr_list_ptr = &mut wr_list[0] as *mut _;
            let qp = unsafe { (*id.id()).qp };
            
            qp_wr_list.push((qp, wr_list_ptr));
        }

        for (qp, wr_list_ptr) in qp_wr_list{
            //let mut bad_wr = unsafe { std::mem::zeroed::<*mut ibv_send_wr>() };
            let bad_wr = null_mut();
            println!("posting send");
            let ret = unsafe { ibv_post_send(qp, wr_list_ptr, bad_wr) };
            if ret != 0 {
                return Err(CustomError::new("ibv_post_send".to_string(), ret).into());
            }
            println!("send posted");
        }
        */

        /*
        for i in 0..iterations{
            let mut jh_list = Vec::new();
            for qp_idx in 0..qps_to_use{
                let mut data_list = data_list.clone();
                let data_addr = data_addr.clone();
                let (mr_addr, id, metadata_request) = data_list.remove(qp_idx as usize);
                let start_offset = qp_idx * messages_per_qp * message_size;
                let rkey = metadata_request.rkey();
                let remote_address = metadata_request.remote_address(); 
                let mr = Mr(mr_addr.mr);
                let jh = tokio::spawn(async move{
                    rdma_write(
                        &id,
                        mr,
                        data_addr,
                        rkey,
                        remote_address,
                        start_offset as usize,
                        message_size as usize,
                        messages_per_qp,
                        qp_idx
                    ).unwrap();
                    if i == iterations -1 {
                        let mut metadata_request = metadata_request;
                        let mr_addr = metadata_request.create_and_register_mr(&id, Operation::SendRecv).unwrap();
                        metadata_request.set_request_type(MetaDataRequestTypes::WriteFinished);
                        metadata_request.rdma_send(&id, &mr_addr).unwrap();
                    }
                });
                jh_list.push(jh);
                //tokio::time::sleep(time::Duration::from_micros(200)).await;

            }
            futures::future::join_all(jh_list).await;
        }
        */
        Ok(())
    }
    /*
    pub fn send(&self, message_size: usize, iterations: usize) -> anyhow::Result<(), CustomError> {
        let mut metadata_request = MetaData::default();
        metadata_request.set_request_type(MetaDataRequestTypes::SendRequest);
        metadata_request.set_message_size(message_size as u32);
        metadata_request.set_iterations(iterations as u32);
        let mr_ar = metadata_request.create_and_register_mr(&self.id, Operation::SendRecv)?;
        metadata_request.rdma_send(&self.id, &mr_ar)?;
        metadata_request.rdma_recv(&self.id, &mr_ar)?;
        match metadata_request.get_request_type(){
            MetaDataRequestTypes::SendResponse => {
                let mut data = Data::new(message_size);
                let data_mr_addr = data.create_and_register_mr(&self.id, Operation::SendRecv)?;
                data.rdma_send_data(&self.id, &data_mr_addr, iterations)?;
                println!("Send finished");
                metadata_request.set_request_type(MetaDataRequestTypes::SendFinished);
                metadata_request.rdma_send(&self.id, &mr_ar)?;
            },
            _ => {
                return Err(CustomError::new("unexpected request type".to_string(), 0).into());
            }
        }
        Ok(())
    }

    pub fn read(&self, message_size: usize, iterations: usize) -> anyhow::Result<(), CustomError> {
        let mut metadata_request = MetaData::default();
        metadata_request.set_request_type(MetaDataRequestTypes::ReadRequest);
        metadata_request.set_message_size(message_size as u32);
        let metadata_mr_addr = metadata_request.create_and_register_mr(&self.id, Operation::SendRecv)?;
        metadata_request.rdma_send(&self.id, &metadata_mr_addr)?;
        metadata_request.rdma_recv(&self.id, &metadata_mr_addr)?;
        match metadata_request.get_request_type(){
            MetaDataRequestTypes::ReadResponse => {
                let mut data = Data::new(message_size);
                data.create_and_register_mr(&self.id, Operation::Read)?;
                data.rdma_read(&self.id, metadata_request.rkey(), metadata_request.remote_address(), iterations)?;
                println!("RDMA Read finished");
                metadata_request.set_request_type(MetaDataRequestTypes::ReadFinished);
                metadata_request.rdma_send(&self.id, &metadata_mr_addr)?;
            },
            _ => {
                return Err(CustomError::new("unexpected request type".to_string(), 0).into());
            }  
        }
        Ok(())
    }
    */
    
}