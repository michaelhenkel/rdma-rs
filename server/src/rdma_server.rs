
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
        let my_id = Arc::new(Mutex::new(MyId(null_mut())));
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
    pub async fn connect(self, address: String, port: u16) -> anyhow::Result<MyId, CustomError>{
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
        Ok(MyId(id))
    }
    pub async fn listen(&self, my_id: Arc<Mutex<MyId>>) -> anyhow::Result<u8, CustomError> {
        println!("listening");
        let my_id_clone = my_id.clone();
        let my_id_lock = my_id_clone.lock().unwrap();
        let id = my_id_lock.0;
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
        let metadata_buffer: *mut MetaData = &mut metadata_request;
        let recv_mr = register_send_recv_mr(id, metadata_buffer.cast(), MetaData::LEN)?;
        let opcode = rdma_recv_md(id, recv_mr, metadata_buffer.cast(), MetaData::LEN)?;
        println!("received metadata request {:?}, opcode: {}", metadata_request, opcode);
        if metadata_request.request_type == 1 {
            let res = self.write(id, recv_mr, metadata_request, metadata_buffer)?;
            return Ok(res);
        }
        if metadata_request.request_type == 4 {
            let res = self.recv(id, recv_mr, metadata_request, metadata_buffer)?;
            return Ok(res);
        }
        Ok(0)
    }
    fn write(&self, id: *mut rdma_cm_id, recv_mr: *mut ibv_mr, metadata_request: MetaData, metadata_buffer: *mut MetaData) -> anyhow::Result<u8, CustomError> {
        let mut data_buffer = vec![0u8; metadata_request.length as usize];
        let write_mr = register_write_mr(id, data_buffer.as_mut_ptr().cast(), data_buffer.len())?;
        unsafe { (*metadata_buffer).request_type = 2 };
        unsafe { (*metadata_buffer).address = (*write_mr).addr as u64 };
        unsafe { (*metadata_buffer).length = data_buffer.len() as u32 };
        unsafe { (*metadata_buffer).rkey = (*write_mr).rkey as u32 };
        rdma_send_md(id, recv_mr, metadata_buffer.cast(), MetaData::LEN)?;
        println!("sent request done {} ", unsafe { (*metadata_buffer).request_type });
        unsafe { (*metadata_buffer).request_type = 5 };
        let opcode = rdma_recv_md(id, recv_mr, metadata_buffer.cast(), MetaData::LEN)?;
        println!("received opcode {}", opcode);
        println!("write request done {} ", unsafe { (*metadata_buffer).request_type });
        Ok(unsafe { (*metadata_buffer).request_type })
    }
    fn recv(&self, id: *mut rdma_cm_id, recv_md_mr: *mut ibv_mr, metadata_request: MetaData, metadata_buffer: *mut MetaData) -> anyhow::Result<u8, CustomError> {
        let mut data_buffer = vec![0u8; metadata_request.length as usize];
        let recv_data_mr = register_send_recv_mr(id, data_buffer.as_mut_ptr().cast(), data_buffer.len())?;
        unsafe { (*metadata_buffer).request_type = 5 };
        rdma_send_md(id, recv_md_mr, metadata_buffer.cast(), MetaData::LEN)?;
        let iterations = unsafe { (*metadata_buffer).iterations };
        rdma_recv_data(id, recv_data_mr, data_buffer.as_mut_ptr().cast(), data_buffer.len(), iterations as usize)?;
        rdma_recv_md(id, recv_md_mr, metadata_buffer.cast(), MetaData::LEN)?;
        println!("recv request done");
        Ok(unsafe { (*metadata_buffer).request_type })
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