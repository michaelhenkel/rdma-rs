use std::ptr::null_mut;
use common::*;
use rdma_sys::*;


pub struct RdmaClient{
    id: *mut rdma_cm_id,
}

impl RdmaClient{
    pub fn new() -> RdmaClient{
        RdmaClient{
            id: null_mut(),
        }
    }

    pub fn connect(&mut self, ip: &str, port: &str) -> anyhow::Result<(), CustomError>{
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
        let ret = unsafe { rdma_create_ep(&mut id, res, null_mut(), &mut attr) };
        if ret != 0 {
            unsafe { rdma_freeaddrinfo(res); }
            return Err(CustomError::new("rdma_create_ep".to_string(), ret).into());
        }

        /*
        let send_cq = unsafe { (*id).send_cq };
        if !send_cq.is_null(){
            println!("send cq: {}", unsafe { (*send_cq).cqe });
            let ret = unsafe { ibv_resize_cq(send_cq, 65535)};
            if ret != 0 {
                return Err(CustomError::new("ibv_resize_cq".to_string(), ret).into());
            }
        }
        println!("send cq: {}", unsafe { (*send_cq).cqe });

        let recv_cq = unsafe { (*id).recv_cq };
        if !recv_cq.is_null(){
            println!("recv cq: {}", unsafe { (*recv_cq).cqe });
            let ret = unsafe { ibv_resize_cq(recv_cq, 65535)};
            if ret != 0 {
                return Err(CustomError::new("ibv_resize_cq".to_string(), ret).into());
            }
        }
        println!("recv cq: {}", unsafe { (*recv_cq).cqe });
        */

        let ret = unsafe { rdma_connect(id, null_mut()) };
        if ret != 0 {
            unsafe { rdma_disconnect(id); }
            return Err(CustomError::new("rdma_connect".to_string(), ret).into());
        }
        self.id = id;
        Ok(())
    }

    pub fn disconnect(&self) -> anyhow::Result<(), CustomError>{
        let mut metadata_request = MetaData::default();
        metadata_request.request_type = 0;
        let metadata_request_ptr: *mut MetaData = &mut metadata_request;
        let send_mr = register_send_recv_mr(self.id, metadata_request_ptr.cast(), MetaData::LEN)?;
        rdma_send_md(self.id, send_mr, metadata_request_ptr.cast(), MetaData::LEN)?;
        println!("sent disconnect");
        Ok(())
    }

    pub fn write(&self, message_size: usize, iterations: usize) -> anyhow::Result<(), CustomError> {
        let mut metadata_request = MetaData::default();
        metadata_request.request_type = 1;
        metadata_request.length = message_size as u32;
        let metadata_request_ptr: *mut MetaData = &mut metadata_request;
        let send_mr = register_send_recv_mr(self.id, metadata_request_ptr.cast(), MetaData::LEN)?;
        rdma_send_md(self.id, send_mr, metadata_request_ptr.cast(), MetaData::LEN)?;
        rdma_recv_md(self.id, send_mr, metadata_request_ptr.cast(), MetaData::LEN)?;
        if metadata_request.request_type == 2 {
            let mut data_buffer = vec![1u8; message_size];
            let write_mr = register_write_mr(self.id, data_buffer.as_mut_ptr().cast(), data_buffer.len())?;
            let my_mr = MyMr(write_mr);
            let my_id = MyId(self.id);
            let my_addr = MyAddress(data_buffer.as_mut_ptr().cast());
            rdma_write(my_id, my_mr, my_addr, data_buffer.len(), metadata_request.rkey, metadata_request.address, iterations)?;
            println!("done writing data");
            metadata_request.request_type = 3;
            println!("sending md done {} ", unsafe { (*metadata_request_ptr).request_type });
            rdma_send_md(self.id, send_mr, metadata_request_ptr.cast(), MetaData::LEN)?;
            println!("done sending metadata");
            //rdma_send_md(self.id, send_mr, metadata_request_ptr.cast(), MetaData::LEN)?;
            println!("done sending metadata");
        }
        Ok(())
    }
    pub fn send(&self, message_size: usize, iterations: usize) -> anyhow::Result<(), CustomError> {
        let mut metadata_request = MetaData::default();
        metadata_request.request_type = 4;
        metadata_request.length = message_size as u32;
        metadata_request.iterations = iterations as u32;
        let metadata_request_ptr: *mut MetaData = &mut metadata_request;
        let send_md_mr = register_send_recv_mr(self.id, metadata_request_ptr.cast(), MetaData::LEN)?;
        rdma_send_md(self.id, send_md_mr, metadata_request_ptr.cast(), MetaData::LEN)?;
        rdma_recv_md(self.id, send_md_mr, metadata_request_ptr.cast(), MetaData::LEN)?;
        if metadata_request.request_type == 5 {
            let mut data_buffer = vec![3u8; message_size];
            let send_data_mr = register_send_recv_mr(self.id, data_buffer.as_mut_ptr().cast(), data_buffer.len())?;
            let my_mr = MyMr(send_data_mr);
            let my_id = MyId(self.id);
            let my_addr = MyAddress(data_buffer.as_mut_ptr().cast());
            rdma_send_data(my_id, my_mr, my_addr, data_buffer.len(), iterations)?;
            println!("done sending data");
            unsafe { (*metadata_request_ptr).request_type = 0};
            rdma_send_md(self.id, send_md_mr, metadata_request_ptr.cast(), MetaData::LEN)?;
            println!("done sending metadata");
        }
        Ok(())
    }
}