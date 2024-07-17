use std::{collections::HashMap, os::raw::c_void, ptr::null_mut};
use common::*;
use rdma_sys::*;


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

    pub fn write(&self, message_size: usize, iterations: usize, volume: u64) -> anyhow::Result<(), CustomError> {

        let messages = volume/(message_size as u64);
        let remaining_bytes = volume % (message_size as u64);



        let mut id_data_map = HashMap::new();
        let mut metadata_mr_map = HashMap::new();
        let qps_to_use = if messages > self.id.len() as u64 { self.id.len() as u64 } else { messages };
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
            id_data_map.insert(qp_idx, (mr_addr, id.clone(), metadata_request));
            metadata_mr_map.insert(qp_idx, metadata_mr_addr);
        }

        for msg in 0..messages{
            let qp_idx = msg % qps_to_use;
            let (mr_addr, id, metadata_request) = id_data_map.get_mut(&qp_idx).unwrap();
            let offset = msg * (message_size as u64);
            data.rdma_write(id, mr_addr, metadata_request.rkey(), metadata_request.remote_address(), iterations, offset as usize, message_size)?;
        }

        for qp_idx in 0..qps_to_use{
            let (_data, id, ref mut metadata_request) = id_data_map.get_mut(&qp_idx).unwrap();
            let metadata_mr_addr = metadata_mr_map.get(&qp_idx).unwrap();
            metadata_request.set_request_type(MetaDataRequestTypes::WriteFinished);
            metadata_request.rdma_send(&id, &metadata_mr_addr)?;
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