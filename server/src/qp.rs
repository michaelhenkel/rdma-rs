use std::ptr::null_mut;
use common::{CustomError, Data, Id, InitAttr, MetaData, MetaDataRequestTypes, MrAddr, MrObject, Operation, ProtectionDomain};
use rdma_sys::*;
#[derive(Clone)]
pub struct Qp{
    pub id: Id,
    pub listen_id: Id,
    pub init_attr: InitAttr,
    pub address: String,
    pub port: u16,
}

impl Qp{
    pub fn new(address: String, pd: &mut Option<ProtectionDomain>) -> anyhow::Result<Self, CustomError>{
        let port = portpicker::pick_unused_port().unwrap();
        let rdma_port = format!("{}\0",port);
        let rdma_port = rdma_port.as_str();
        let address = address.as_str();
        let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
        let mut res: *mut rdma_addrinfo = null_mut();
        hints.ai_flags = RAI_PASSIVE.try_into().unwrap();
        hints.ai_port_space = rdma_port_space::RDMA_PS_TCP.try_into().unwrap();
        let ret = unsafe {
            rdma_getaddrinfo(
                address.as_ptr().cast(),
                rdma_port.as_ptr().cast(),
                &hints,
                &mut res,
            )
        };
        if ret != 0 {
            return Err(CustomError::new("rdma_getaddrinfo".to_string(), ret).into());
        }
        let mut listen_id = null_mut();
        let id = null_mut();
        let mut init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        init_attr.cap.max_send_wr = 4096;
        init_attr.cap.max_recv_wr = 4096;
        init_attr.cap.max_send_sge = 1;
        init_attr.cap.max_recv_sge = 1;
        init_attr.cap.max_inline_data = 64;
        init_attr.sq_sig_all = 1;
        
        if pd.is_none(){
            *pd = Some(ProtectionDomain(null_mut()));
        }

        let ret = unsafe { rdma_create_ep(&mut listen_id, res, pd.as_ref().unwrap().pd(), &mut init_attr) };
        if ret != 0 {
            unsafe { rdma_freeaddrinfo(res); }
            return Err(CustomError::new("rdma_create_ep".to_string(), ret).into());
        }
        Ok(Qp{
            id: Id(id),
            listen_id: Id(listen_id),
            init_attr: InitAttr(init_attr),
            address: address.to_string(),
            port,
        })
    }
    pub async fn wait_for_connection(&mut self) -> anyhow::Result<(), CustomError> {
        let mut init_attr = self.init_attr.init_attr();
        let mut id = self.id.id();
        let listen_id = self.listen_id.id();
        let ret = unsafe { rdma_listen(listen_id, 0) };
        if ret != 0 {
            unsafe { rdma_destroy_ep(listen_id); }
            return Err(CustomError::new("rdma_listen".to_string(), ret).into());
        }
        
        let ret = unsafe { rdma_get_request(listen_id, &mut id) };
        if ret != 0 {
            unsafe { rdma_destroy_ep(listen_id); }
            return Err(CustomError::new("rdma_get_request".to_string(), ret).into());
        }
        println!("Connection received, accepting it");
        if id.is_null() {
            return Err(CustomError::new("id is null".to_string(), ret).into());
        }
        let ret = unsafe { rdma_accept(id, null_mut()) };
        if ret != 0 {
            return Err(CustomError::new("rdma_accept".to_string(), ret).into());
        }
        println!("Connection accepted");
        let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
        let ret = unsafe {
            ibv_query_qp(
                (*id).qp,
                &mut qp_attr,
                ibv_qp_attr_mask::IBV_QP_CAP.0.try_into().unwrap(),
                &mut init_attr,
            )
        };
        if ret != 0 {
            unsafe { rdma_destroy_ep(id); }
            return Err(CustomError::new("ibv_query_qp".to_string(), ret).into());
        }
        qp_attr.timeout = 14;
        unsafe { ibv_modify_qp((*id).qp, &mut qp_attr, ibv_qp_attr_mask::IBV_QP_TIMEOUT.0 as i32) };
        if id.is_null() {
            return Err(CustomError::new("rdma_get_request".to_string(), ret).into());
        }
        self.id = Id(id);
        Ok(())
    }
    pub async fn listen(&mut self) -> anyhow::Result<u8, CustomError>{
        println!("Listening");
        let id = self.id.clone();     
        let mut metadata_request = MetaData::default();
        let metadata_mr_addr = metadata_request.create_and_register_mr(&id, Operation::SendRecv)?;
        let mut data = None;
        metadata_request.rdma_recv(&id, &metadata_mr_addr)?;
        println!("{:?}", metadata_request.get_request_type());
        match metadata_request.get_request_type(){
            MetaDataRequestTypes::WriteRequest => {

                if data.is_none(){
                    data = Some(Data::new(metadata_request.message_size() as usize));
                }

                let mr = unsafe { rdma_reg_write(id.id(), data.as_mut().unwrap().addr(), data.as_ref().unwrap().len()) };
                if mr.is_null(){
                    return Err(CustomError::new("rdma_reg_write".to_string(), -1).into());
                }
                let mr_addr = unsafe { (*mr).addr as u64 };
                let mr_rkey = unsafe { (*mr).rkey as u32 };
   
                metadata_request.set_request_type(MetaDataRequestTypes::WriteResponse);
                metadata_request.set_remote_address(mr_addr);
                metadata_request.set_rkey(mr_rkey);
                metadata_request.rdma_send(&id, &metadata_mr_addr)?;
                metadata_request.rdma_recv(&id, &metadata_mr_addr)?;
                return Ok(metadata_request.get_request_type() as u8);
            },
            MetaDataRequestTypes::SendRequest => {
                let mut data = Data::new(metadata_request.message_size() as usize);
                let data_mr_addr = data.create_and_register_mr(&id, Operation::SendRecv)?;
                metadata_request.set_request_type(MetaDataRequestTypes::SendResponse);
                metadata_request.rdma_send(&id, &metadata_mr_addr)?;
                data.rdma_recv_data(&id, &data_mr_addr, metadata_request.iterations() as usize)?;
                metadata_request.rdma_recv(&id, &metadata_mr_addr)?;
                return Ok(metadata_request.get_request_type() as u8);
            },
            MetaDataRequestTypes::ReadRequest => {
                let mut data = Data::new(metadata_request.message_size() as usize);
                data.create_and_register_mr(&id, Operation::Read)?;
                metadata_request.set_request_type(MetaDataRequestTypes::ReadResponse);
                metadata_request.set_remote_address(data.mr_addr());
                metadata_request.set_rkey(data.mr_rkey());
                metadata_request.rdma_send(&id, &metadata_mr_addr)?;
                metadata_request.rdma_recv(&id, &metadata_mr_addr)?;
                return Ok(metadata_request.get_request_type() as u8);
            },
            MetaDataRequestTypes::Disconnect => {
                return Ok(0);
            },
            MetaDataRequestTypes::WriteFinished => {
                println!("Write finished");
                return Ok(0);
            },
            _ => {
                return Ok(0);
            }
        }
    }
}