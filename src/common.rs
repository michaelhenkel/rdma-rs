use std::{fmt::Display, ptr::{self, null_mut}};
use libc::{c_int, c_void};
use rdma_sys::*;

const BATCH_SIZE: usize = 2000;

#[derive(Debug)]
pub struct CustomError{
    message: String,
    code: i32
}

impl CustomError{
    pub fn new(message: String, code: i32) -> CustomError{
        CustomError{message, code}
    }
    pub fn code(&self) -> i32{
        self.code
    }
}

impl Display for CustomError{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result{
        write!(f, "Error: {} with code: {}", self.message, self.code)
    }
}

pub unsafe fn send_complete(id: Id, iterations: usize, opcode_type: ibv_wc_opcode::Type) -> anyhow::Result<i32, CustomError>{
    let mut ret: c_int;
    let mut cq = ptr::null::<ibv_cq>() as *mut _;
    let mut context = ptr::null::<c_void>() as *mut _;

    let mut wc_vec: Vec<ibv_wc> = Vec::with_capacity(BATCH_SIZE);
    let wc_ptr = wc_vec.as_mut_ptr();

    let mut total_wc: i32 = 0;

    let nevents = 1;
    let solicited_only = 0;

    loop {
        ret = ibv_poll_cq((*id.0).send_cq, BATCH_SIZE as i32, wc_ptr.wrapping_add(total_wc as usize));
        if ret < 0 {
            return Err(CustomError::new("ibv_poll_cq".to_string(), ret).into());
        }
        total_wc += ret;
        if total_wc >= iterations as i32{
            break;
        }
        ret = ibv_req_notify_cq((*id.0).send_cq, solicited_only);
        if ret != 0 {
            return Err(CustomError::new("ibv_req_notify_cq".to_string(), ret).into());
        }
        ret = ibv_poll_cq((*id.0).send_cq, BATCH_SIZE as i32, wc_ptr.wrapping_add(total_wc as usize));
        if ret < 0 {
            return Err(CustomError::new("ibv_poll_cq".to_string(), ret).into());
        }
        total_wc += ret;
        if total_wc >= iterations as i32{
            break;
        }
        ret = ibv_get_cq_event((*id.0).send_cq_channel, &mut cq, &mut context);
        if ret != 0 {
            return Err(CustomError::new("ibv_get_cq_event".to_string(), ret).into());
        }
        assert!(cq == (*id.0).send_cq && context as *mut rdma_cm_id == id.0);
        ibv_ack_cq_events((*id.0).send_cq, nevents);
    }
    if ret < 0 {
        return Err(CustomError::new("ibv_poll_cq".to_string(), ret).into());
    }
    for i in 0..total_wc{
        let wc = wc_ptr.wrapping_add(i as usize);
        let status = (*wc).status;
        let opcode = (*wc).opcode;
        if status != ibv_wc_status::IBV_WC_SUCCESS || opcode != opcode_type{
            return Err(CustomError::new(format!("wc status/opcode {}/{} wrong, expected {}/{}", status, opcode, ibv_wc_status::IBV_WC_SUCCESS, opcode_type).to_string(), -1).into());
        }
    }
    Ok(total_wc)
}

pub unsafe fn recv_complete(id: Id, iterations: usize) -> anyhow::Result<i32, CustomError>{
    let mut ret: c_int;
    let mut cq = ptr::null::<ibv_cq>() as *mut _;
    let mut context = ptr::null::<c_void>() as *mut _;

    let mut wc_vec: Vec<ibv_wc> = Vec::with_capacity(BATCH_SIZE);
    let wc_ptr = wc_vec.as_mut_ptr();

    let mut total_wc = 0;

    let nevents = 1;
    let solicited_only = 0;

    loop {
        ret = ibv_poll_cq((*id.0).recv_cq, BATCH_SIZE as i32, wc_ptr);
        if ret < 0 {
            return Err(CustomError::new("ibv_poll_cq".to_string(), ret).into());
        }
        total_wc += ret;
        if total_wc == iterations as i32{
            break;
        }
        ret = ibv_req_notify_cq((*id.0).recv_cq, solicited_only);
        if ret != 0 {
            return Err(CustomError::new("ibv_req_notify_cq".to_string(), ret).into());
        }
        ret = ibv_poll_cq((*id.0).recv_cq, BATCH_SIZE as i32, wc_ptr);
        if ret < 0 {
            return Err(CustomError::new("ibv_poll_cq".to_string(), ret).into());
        }
        total_wc += ret;
        if total_wc == iterations as i32{
            break;
        }
        ret = ibv_get_cq_event((*id.0).recv_cq_channel, &mut cq, &mut context);
        if ret != 0 {
            return Err(CustomError::new("ibv_get_cq_event".to_string(), ret).into());
        }
        assert!(cq == (*id.0).recv_cq && context as *mut rdma_cm_id == id.0);
        ibv_ack_cq_events((*id.0).recv_cq, nevents);
    }
    if ret < 0 {
        return Err(CustomError::new("ibv_poll_cq".to_string(), ret).into());
    }
    for i in 0..total_wc{
        let wc = wc_ptr.wrapping_add(i as usize);
        let status = (*wc).status;
        if status != ibv_wc_status::IBV_WC_SUCCESS {
            return Err(CustomError::new("wc status or opcode wrong".to_string(), -1).into());
        }
    }
    Ok(total_wc)
}

#[derive(Clone)]
pub struct Id(pub *mut rdma_sys::rdma_cm_id);
unsafe impl Send for Id{}
unsafe impl Sync for Id{}
impl Id{
    pub fn id(&self) -> *mut rdma_sys::rdma_cm_id{
        self.0
    }
}

pub struct Address(pub *mut c_void);
unsafe impl Send for Address{}
unsafe impl Sync for Address{}
impl Address{
    pub fn addr(&self) -> *mut c_void{
        self.0
    }
}

pub struct Mr(pub *mut ibv_mr);
unsafe impl Send for Mr{}
unsafe impl Sync for Mr{}
impl Mr{
    pub fn mr(&self) -> *mut ibv_mr{
        self.0
    }
}

pub fn process_rdma_cm_event(echannel: *mut rdma_event_channel, expected_event: rdma_cm_event_type::Type, rdma_event: *mut *mut rdma_cm_event) -> anyhow::Result<(), CustomError> {
    let res = unsafe { rdma_get_cm_event(echannel, rdma_event) };
    if res != 0 {
        return Err(CustomError::new("rdma_get_cm_event".to_string(), res).into());
    }
    if unsafe { (**rdma_event).status } != 0 {
        unsafe { rdma_ack_cm_event(*rdma_event);}
        println!("{}", unsafe { (**rdma_event).status });
        println!("{}", unsafe { (**rdma_event).event });
        return Err(CustomError::new("CM event has non zero status".to_string(), unsafe { (**rdma_event).status }).into());
    }
    if unsafe { (**rdma_event).event } != expected_event {
        unsafe { rdma_ack_cm_event(*rdma_event);}
        return Err(CustomError::new("Unexpected event received".to_string(), -1).into());
    }
    Ok(())
}


pub struct Data{
    pub buffer: Vec<u8>,
    mr: *mut ibv_mr,
}

impl Data{
    pub fn new(size: usize) -> Data{
        Data{
            buffer: vec![1u8; size],
            mr: null_mut(),
        }
    }
    pub fn buffer(&self) -> Vec<u8>{
        self.buffer.clone()
    }
}

impl MrObject for Data{
    fn len(&self) -> usize{
        self.buffer.len()
    }
    fn addr(&mut self) -> *mut c_void{
        self.buffer.as_mut_ptr().cast()
    }
    fn set_mr(&mut self, mr: *mut ibv_mr){
        self.mr = mr;
    }
    fn mr(&self) -> *mut ibv_mr{
        self.mr
    }
}

pub struct MrAddr{
    pub mr: *mut ibv_mr,
    pub addr: *mut c_void,
}

pub trait MrObject{
    fn mr_rkey(&self) -> u32{
        unsafe { (*self.mr()).rkey as u32 }
    }
    fn mr_addr(&self) -> u64{
        unsafe { (*self.mr()).addr as u64 }
    }
    fn len(&self) -> usize;
    fn addr(&mut self) -> *mut c_void;
    fn set_mr(&mut self, mr: *mut ibv_mr);
    fn mr(&self) -> *mut ibv_mr;
    fn create_and_register_mr(&mut self, id: &Id, operation: Operation) -> anyhow::Result<MrAddr, CustomError> {
        if id.id().is_null(){
            return Err(CustomError::new("id is null".to_string(), -1).into());
        }
        let length = self.len();
        let addr: *mut c_void = self.addr();
        let mr = match operation{
            Operation::SendRecv => {
                unsafe { rdma_reg_msgs(id.id(), addr, length) }
            },
            Operation::Write => {
                unsafe { rdma_reg_write(id.id(), addr, length) }
            },
            Operation::Read => {
                unsafe { rdma_reg_read(id.id(), addr, length) }
            }
        };
        if mr.is_null() {
            unsafe { rdma_dereg_mr(mr); }
            return Err(CustomError::new("rdma_reg_msgs".to_string(), -1).into());
        }
        self.set_mr(mr);
        Ok(MrAddr{mr, addr})
    }
    fn rdma_send(&mut self, id: &Id, mr_addr: &MrAddr) -> anyhow::Result<(), CustomError>{
        let flags = ibv_send_flags::IBV_SEND_INLINE.0 | ibv_send_flags::IBV_SEND_SIGNALED.0;
        let mut ret = unsafe {
            rdma_post_send(
                id.id(),
                null_mut(),
                mr_addr.addr,
                self.len(),
                mr_addr.mr,
                flags as i32,
            )
        };
        if ret != 0 {
            unsafe { rdma_disconnect(id.id()) };
            return Err(CustomError::new("rdma_post_send".to_string(), ret).into());
        }
        let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
        while ret == 0 {
            ret = unsafe { rdma_get_send_comp(id.id(), &mut wc) };
        }
        if ret < 0 {
            println!("rdma_get_send_comp");
        }
        if wc.status != ibv_wc_status::IBV_WC_SUCCESS || wc.opcode != ibv_wc_opcode::IBV_WC_SEND{
            return Err(CustomError::new("wc status or opcode wrong".to_string(), -1).into());
        }
        Ok(())
    }
    fn rdma_recv(&mut self, id: &Id, mr_addr: &MrAddr) -> anyhow::Result<(), CustomError>{
        let mut ret = unsafe { rdma_post_recv(id.id(), null_mut(), mr_addr.addr, self.len(), mr_addr.mr) };
        if ret != 0 {
            unsafe { rdma_dereg_mr(self.mr()) };
            return Err(CustomError::new("rdma_post_recv".to_string(), ret).into());
        }
        let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
        while ret == 0 {
            ret = unsafe { rdma_get_recv_comp(id.id(), &mut wc) };
        }
        if ret < 0 {
            unsafe { rdma_disconnect(id.id()); }
            return Err(CustomError::new("rdma_get_recv_comp".to_string(), ret).into());
        }
        if wc.status != ibv_wc_status::IBV_WC_SUCCESS || wc.opcode != ibv_wc_opcode::IBV_WC_RECV{
            return Err(CustomError::new(format!("wc status/opcode {}/{} wrong, expected {}/{}", wc.status, wc.opcode, ibv_wc_status::IBV_WC_SUCCESS, ibv_wc_opcode::IBV_WC_RECV).to_string(), -1).into());
        }
        Ok(())
    }
    fn rdma_write(&mut self, id: &Id, rkey: u32, remote_addr: u64, iterations: usize) -> anyhow::Result<(), CustomError>{
        let mut flags = 0;
        let mut ret;
        let mut comp = false;
        for i in 1..iterations+1{
            if i == iterations || i % BATCH_SIZE == 0{
                flags = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0 | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0 | ibv_send_flags::IBV_SEND_SIGNALED.0;
                comp = true;
            }
            ret = unsafe {
                rdma_post_write(
                    id.0,
                    null_mut(),
                    self.addr(),
                    self.len(),
                    self.mr(),
                    flags as i32,
                    remote_addr,
                    rkey
                )
            };
            if ret != 0 {
                unsafe { rdma_disconnect(id.0) };
                return Err(CustomError::new("rdma_post_write".to_string(), ret).into());
            }
            
            if comp{
                let _ret = unsafe { send_complete(id.clone(), 1, ibv_wc_opcode::IBV_WC_RDMA_WRITE)? };
                comp = false;
                flags = 0;
            }
        }
        Ok(())
    }
    fn rdma_read(&mut self, id: &Id, rkey: u32, remote_addr: u64, iterations: usize) -> anyhow::Result<(), CustomError>{
        let mut flags = 0;
        let mut ret;
        let mut comp = false;
        for i in 1..iterations+1{
            if i == iterations || i % BATCH_SIZE == 0{
                flags = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0 | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0 | ibv_send_flags::IBV_SEND_SIGNALED.0;
                comp = true;
            }
            ret = unsafe {
                rdma_post_read(
                    id.0,
                    null_mut(),
                    self.addr(),
                    self.len(),
                    self.mr(),
                    flags as i32,
                    remote_addr,
                    rkey
                )
            };
            if ret != 0 {
                unsafe { rdma_disconnect(id.0) };
                return Err(CustomError::new("rdma_post_write".to_string(), ret).into());
            }
            
            if comp{
                let _ret = unsafe { send_complete(id.clone(), 1, ibv_wc_opcode::IBV_WC_RDMA_READ)? };
                comp = false;
                flags = 0;
            }
        }
        Ok(())
    }
    fn rdma_recv_data(&mut self, id: &Id, mr_addr: &MrAddr, iterations: usize) -> anyhow::Result<(), CustomError>{
        let mut ret;
        let mut comp = false;
        for i in 1..iterations+1{
            if i == iterations || i % BATCH_SIZE == 0{
                comp = true;
            }
            ret = unsafe {
                rdma_post_recv(
                    id.id(),
                    null_mut(),
                    mr_addr.addr,
                    self.len(),
                    mr_addr.mr
                )
            };
            if ret != 0 {
                unsafe { rdma_disconnect(id.id()) };
                return Err(CustomError::new("rdma_post_write".to_string(), ret).into());
            }
        
            if comp{
                unsafe { recv_complete(id.clone(), i)? };
                comp = false;
            }
        }
        Ok(())
    }
    fn rdma_send_data(&mut self, id: &Id, mr_addr: &MrAddr, iterations: usize) -> anyhow::Result<(), CustomError>{
        let mut flags = 0;
        let mut ret;
        let mut comp = false;
        for i in 1..iterations+1{
            if i == iterations || i % BATCH_SIZE == 0{
                flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
                comp = true;
            }
            ret = unsafe {
                rdma_post_send(
                    id.0,
                    null_mut(),
                    mr_addr.addr,
                    self.len(),
                    mr_addr.mr,
                    flags as i32
                )
            };
    
            if ret != 0 {
                unsafe { rdma_disconnect(id.0) };
                return Err(CustomError::new("rdma_post_write".to_string(), ret).into());
            }
            if comp{
                let _ret = unsafe { send_complete(id.clone(), 1, ibv_wc_opcode::IBV_WC_SEND)? };
                comp = false;
                flags = 0;
            }
        }
        Ok(())
    }
}



pub enum Operation{
    SendRecv,
    Write,
    Read,
}

#[derive(Debug)]
pub struct MetaData{
    pub request_type: u8,
    pub remote_address: u64,
    pub message_size: u32,
    pub rkey: u32,
    pub lkey: u32,
    pub iterations: u32,
    mr: *mut ibv_mr,
}

impl Default for MetaData{
    fn default() -> MetaData{
        MetaData{
            request_type: 0,
            remote_address: 0,
            message_size: 0,
            rkey: 0,
            lkey: 0,
            iterations: 0,
            mr: null_mut(),
        }
    }
}

impl MetaData{
    pub const LEN: usize = std::mem::size_of::<MetaData>();
    pub fn get_request_type(&self) -> MetaDataRequestTypes{
        match self.request_type{
            0 => MetaDataRequestTypes::Disconnect,
            1 => MetaDataRequestTypes::WriteRequest,
            2 => MetaDataRequestTypes::WriteResponse,
            3 => MetaDataRequestTypes::WriteFinished,
            4 => MetaDataRequestTypes::SendRequest,
            5 => MetaDataRequestTypes::SendResponse,
            6 => MetaDataRequestTypes::SendFinished,
            7 => MetaDataRequestTypes::ReadRequest,
            8 => MetaDataRequestTypes::ReadResponse,
            9 => MetaDataRequestTypes::ReadFinished,
            _ => MetaDataRequestTypes::UnDef,
        }
    }
    pub fn set_request_type(&mut self, request_type: MetaDataRequestTypes){
        match request_type{
            MetaDataRequestTypes::Disconnect => {
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 0 };
                self.request_type = 0
            },
            MetaDataRequestTypes::WriteRequest => {
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 1 };
                self.request_type = 1
            },
            MetaDataRequestTypes::WriteResponse => { 
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 2 };
                self.request_type = 2
            },
            MetaDataRequestTypes::WriteFinished => {
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 3 };
                self.request_type = 3
            },
            MetaDataRequestTypes::SendRequest => {
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 4 };
                self.request_type = 4
            },
            MetaDataRequestTypes::SendResponse => {
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 5 };
                self.request_type = 5
            }
            MetaDataRequestTypes::SendFinished => {
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 6 };
                self.request_type = 6
            },
            MetaDataRequestTypes::ReadRequest => {
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 7 };
                self.request_type = 7
            },
            MetaDataRequestTypes::ReadResponse => {
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 8 };
                self.request_type = 8
            },
            MetaDataRequestTypes::ReadFinished => {
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 9 };
                self.request_type = 9
            },
            MetaDataRequestTypes::UnDef => {
                let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
                unsafe { (*metadata_buffer).request_type = 128 };
                self.request_type = 128
            },
        }
    }
    pub fn set_message_size(&mut self, length: u32){
        let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
        unsafe { (*metadata_buffer).message_size = length };
        self.message_size = length;
    }
    pub fn set_rkey(&mut self, rkey: u32){
        let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
        unsafe { (*metadata_buffer).rkey = rkey };
        self.rkey = rkey;
    }
    pub fn set_remote_address(&mut self, remote_address: u64){
        let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
        unsafe { (*metadata_buffer).remote_address = remote_address };
        self.remote_address = remote_address;
    }
    pub fn set_iterations(&mut self, iterations: u32){
        let metadata_buffer: *mut MetaData = self.addr() as *const _ as *mut MetaData;
        unsafe { (*metadata_buffer).iterations = iterations };
        self.iterations = iterations;
    }
    pub fn rkey(&self) -> u32{
        self.rkey
    }
    pub fn remote_address(&self) -> u64{
        self.remote_address
    }
    pub fn message_size(&self) -> u32{
        self.message_size
    }
    pub fn iterations(&self) -> u32{
        self.iterations
    }
}

impl MrObject for MetaData{
    fn len(&self) -> usize{
        MetaData::LEN
    }
    fn addr(&mut self) -> *mut c_void {
        self as *const _ as *mut c_void
    }
    fn set_mr(&mut self, mr: *mut ibv_mr){
        self.mr = mr;
    }
    fn mr(&self) -> *mut ibv_mr{
        self.mr
    }
}

#[derive(Debug)]
pub enum MetaDataRequestTypes{
    Disconnect = 0,
    WriteRequest = 1,
    WriteResponse = 2,
    WriteFinished = 3,
    SendRequest = 4,
    SendResponse = 5,
    SendFinished = 6,
    ReadRequest = 7,
    ReadResponse = 8,
    ReadFinished = 9,
    UnDef = 128,
}