use std::{collections::HashMap, ptr::null_mut, sync::{Arc, Mutex}};
use common::*;
use rdma_sys::*;

const BATCH_SIZE: usize = 2000;

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
        attr.cap.max_send_sge = 15;
        attr.cap.max_recv_sge = 15;
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
        let _remaining_bytes = volume % message_size;

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

        let mut sge_map = HashMap::new();
        let mut offset = 0;
        for qp_idx in 0..qps_to_use{
            let (mr_addr, _id, _metadata_request) = data_list.get_mut(qp_idx as usize).unwrap();
            let mut sge_list = Vec::new();
            for _ in 0..messages_per_qp{
                let mut sge = unsafe { std::mem::zeroed::<ibv_sge>() };
                let addr = unsafe { (*mr_addr.mr).addr };
                sge.addr = addr.wrapping_add(offset as usize) as u64;
                sge.length = message_size as u32;
                sge.lkey = unsafe { (*mr_addr.mr).lkey };
                sge_list.push(sge);
                offset += message_size;
            }
            sge_map.insert(qp_idx, sge_list);
        }

        
        //let mut id_wr_list = Vec::new();
        //let mut qp_list = Vec::new();


        let mut id_wr_list = Vec::new();
        for _ in 0..iterations{
            let mut qp_list = Vec::new();
            for _ in 0..qps_to_use{
                qp_list.push(Vec::new());
            }
            id_wr_list.push(qp_list);
        }


        let mut id_map = HashMap::new();
        for it in 0..iterations{
            for qp_idx in 0..qps_to_use{
                let (_mr_addr, id, metadata_request) = data_list.get_mut(qp_idx as usize).unwrap();
                let mut id_wr = IdWr::new(id.clone());
                let id = id.clone();
                id_map.insert(qp_idx, id.clone());
                let sge_list = sge_map.get_mut(&qp_idx).unwrap();
                let mut wr_idx = 0;
                for msg in 0..messages_per_qp{
                    let offset = qp_idx * messages_per_qp * message_size + msg * message_size;
                    let sge = sge_list.get_mut(wr_idx).unwrap();
                    let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
                    wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
                    wr.sg_list = &mut *sge;
                    wr.num_sge = 1;
                    let remote_addr = metadata_request.remote_address();
                    let remote_addr = remote_addr.wrapping_add(offset);
                    wr.wr.rdma.remote_addr = remote_addr;
                    wr.wr.rdma.rkey = metadata_request.rkey();
                    if msg + 1 == messages_per_qp || (msg + 1) as usize % BATCH_SIZE == 0{
                        wr.send_flags =  ibv_send_flags::IBV_SEND_SIGNALED.0;
                    } else {
                        wr.send_flags = 0;
                    }
                    id_wr.add_wr(IbvSendWr(wr));
                    if msg + 1 == messages_per_qp || (msg + 1) as usize % BATCH_SIZE == 0{
                        //let id_wr_len = id_wr.wr_list.len();
                        id_wr.set_wr_ptr_list();
                        id_wr_list[it][qp_idx as usize].push(id_wr);
                        //println!("iter: {}, qp_idx: {}, msg: {}, messages_per_qp {}, wrs: {}, msg per wr: {}", it, qp_idx, msg + 1, messages_per_qp, id_wr_list[it][qp_idx as usize].len(), id_wr_len);
                        id_wr = IdWr::new(id.clone());
                    }
                    wr_idx += 1;
                }
            }
        }

        let iter_now = tokio::time::Instant::now();

        while let Some(mut qp_list) = id_wr_list.pop(){
            let mut jh_list = Vec::new();
            let now = tokio::time::Instant::now();
            while let Some(id_wr_list) = qp_list.pop(){

                let id_wr_list = Arc::new(Mutex::new(id_wr_list));
                let jh = tokio::spawn(async move {
                    let id_wr_list = id_wr_list.clone();
                    let mut id_wr_list = id_wr_list.lock().unwrap();
                    while let Some(mut id_wr) = id_wr_list.pop(){
                        let first_wr = &mut id_wr.wr_list.get_mut(0).unwrap().0;
                        let id = id_wr.id.clone();
                        let qp = unsafe { (*id.id()).qp };
                        let mut bad_wr = null_mut();
                        let ret = unsafe { ibv_post_send(qp, &mut *first_wr, &mut bad_wr) };
                        if ret != 0 {
                            println!("ibv_post_send failed");
                        }
                        unsafe { send_complete(id, 1, ibv_wc_opcode::IBV_WC_RDMA_WRITE) }.unwrap();
                    }

                });
                jh_list.push(jh);
            }
            futures::future::join_all(jh_list).await;
            let elapsed = now.elapsed();
            let message_size_byte = byte_unit::Byte::from_u64(volume);
            let message_size_string = format!("{message_size_byte:#}");
            let gbit = (volume * 8) as f64 / 1_000_000_000.0;
            let gbps = gbit / elapsed.as_secs_f64();
            println!("per iter: wrote {} in {:.2}s: {:.2} Gbps",message_size_string, elapsed.as_secs_f64(), gbps);

        }

        let iter_elapsed = iter_now.elapsed();
        let message_size_byte = byte_unit::Byte::from_u64(volume * (iterations as u64));
        let message_size_string = format!("{message_size_byte:#}");
        let gbit = (volume * (iterations as u64) * 8) as f64 / 1_000_000_000.0;
        let gbps = gbit / iter_elapsed.as_secs_f64();
        println!("total: wrote {} in {:.2}s: {:.2} Gbps",message_size_string, iter_elapsed.as_secs_f64(), gbps);

        for id in id_map.values(){
            let mut metadata_request = MetaData::default();
            metadata_request.set_request_type(MetaDataRequestTypes::WriteFinished);
            let mr_addr = metadata_request.create_and_register_mr(&id, Operation::SendRecv).unwrap();
            metadata_request.rdma_send(&id, &mr_addr).unwrap();
        }
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