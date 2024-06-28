
use std::{ptr::null_mut, sync::{Arc, Mutex}};
use rdma_sys::*;
use common::*;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct RdmaServer{
    pub client: RdmaServerClient,
    rx: Arc<RwLock<tokio::sync::mpsc::Receiver<RdmaServerCommand>>>
}

impl RdmaServer{
    pub fn new() -> RdmaServer{
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let client = RdmaServerClient::new(tx);
        RdmaServer{
            client,
            rx: Arc::new(RwLock::new(rx))
        }
    }
    pub async fn run(&self) -> anyhow::Result<()>{
        let mut rx = self.rx.write().await;
        let my_id = Arc::new(Mutex::new(Id(null_mut())));
        while let Some(rdma_server_command) = rx.recv().await{
            let rdma_server = self.clone();
            match rdma_server_command{
                RdmaServerCommand::Listen{tx} => {
                    loop {
                        let ret = rdma_server.listen(my_id.clone()).await.unwrap();
                        if ret == 0 {
                            break;
                        }
                    }
                    tx.send(()).unwrap();
                },
                RdmaServerCommand::Connect{address, port} => {
                    let id = rdma_server.connect(address, port).await.unwrap();
                    let mut my_id = my_id.lock().unwrap();
                    *my_id = id;
                }
            }
        }
        println!("rdma server stopped");
        Ok(())
    }
    pub async fn connect(self, address: String, port: u16) -> anyhow::Result<Id, CustomError>{
        let port = format!("{}\0",port);
        let address = format!("{}\0",address);
        let port = port.as_str();
        let address = address.as_str();
        let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
        let mut res: *mut rdma_addrinfo = null_mut();
        hints.ai_flags = RAI_PASSIVE.try_into().unwrap();
        hints.ai_port_space = rdma_port_space::RDMA_PS_TCP.try_into().unwrap();
        let ret = unsafe {
            rdma_getaddrinfo(
                address.as_ptr().cast(),
                port.as_ptr().cast(),
                &hints,
                &mut res,
            )
        };
        if ret != 0 {
            return Err(CustomError::new("rdma_getaddrinfo".to_string(), ret).into());
        }
        let mut listen_id = null_mut();
        let mut id = null_mut();
    
        let mut init_attr = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
        init_attr.cap.max_send_wr = 4096;
        init_attr.cap.max_recv_wr = 4096;
        init_attr.cap.max_send_sge = 1;
        init_attr.cap.max_recv_sge = 1;
        init_attr.cap.max_inline_data = 64;
        init_attr.sq_sig_all = 1;
        let ret = unsafe { rdma_create_ep(&mut listen_id, res, null_mut(), &mut init_attr) };
        if ret != 0 {
            unsafe { rdma_freeaddrinfo(res); }
            return Err(CustomError::new("rdma_create_ep".to_string(), ret).into());
        }
        println!("Waiting for connection");
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
        let ret = unsafe { rdma_accept(id, null_mut()) };
        if ret != 0 {
            return Err(CustomError::new("rdma_accept".to_string(), ret).into());
        }

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
        Ok(Id(id))
    }
    pub async fn listen(&self, my_id: Arc<Mutex<Id>>) -> anyhow::Result<u8, CustomError> {
        let my_id_clone = my_id.clone();
        let my_id_lock = my_id_clone.lock().unwrap();
        let id = Id(my_id_lock.0);
        /* 
        let recv_cq = unsafe { (*id).recv_cq };
        if !recv_cq.is_null(){
            println!("recv cq: {}", unsafe { (*recv_cq).cqe });
            let ret = unsafe { ibv_resize_cq(recv_cq, 65535)};
            if ret != 0 {
                return Err(CustomError::new("ibv_resize_cq".to_string(), ret).into());
            }
            println!("recv cq: {}", unsafe { (*recv_cq).cqe });
        } else {
            println!("recv cq is null");
        }
        let send_cq = unsafe { (*id).send_cq };
        if !send_cq.is_null(){
            println!("send cq: {}", unsafe { (*send_cq).cqe });
            let ret = unsafe { ibv_resize_cq(send_cq, 65535)};
            if ret != 0 {
                return Err(CustomError::new("ibv_resize_cq".to_string(), ret).into());
            }
            println!("send cq: {}", unsafe { (*send_cq).cqe });
        } else {
            println!("send cq is null");
        }
        */
        
        let mut metadata_request = MetaData::default();
        let metadata_mr_addr = metadata_request.create_and_register_mr(&id, Operation::SendRecv)?;
        metadata_request.rdma_recv(&id, &metadata_mr_addr)?;
        println!("{:?}", metadata_request.get_request_type());
        match metadata_request.get_request_type(){
            MetaDataRequestTypes::WriteRequest => {
                let mut data = Data::new(metadata_request.message_size() as usize);
                data.create_and_register_mr(&id, Operation::Write)?;
                metadata_request.set_request_type(MetaDataRequestTypes::WriteResponse);
                metadata_request.set_remote_address(data.mr_addr());
                metadata_request.set_rkey(data.mr_rkey());
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
            _ => {
                return Ok(0);
            }
        }
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
    pub async fn listen(&mut self) -> anyhow::Result<()>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(RdmaServerCommand::Listen{tx}).await.unwrap();
        rx.await.unwrap();
        Ok(())
    }
    pub async fn connect(&mut self, address: String, port: u16) -> anyhow::Result<()>{
        match self.tx.send(RdmaServerCommand::Connect{address, port}).await{
            Ok(_) => {},
            Err(e) => {
                println!("error: {}",e);
            }
        
        }
        Ok(())
    }

}

pub enum RdmaServerCommand{
    Listen{
        tx: tokio::sync::oneshot::Sender<()>
    },
    Connect{
        address: String,
        port: u16,
    }
}