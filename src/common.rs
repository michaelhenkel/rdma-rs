use std::{collections::BTreeMap, ffi::CStr, fmt::Display, fs, net::{Ipv4Addr, Ipv6Addr}, path::PathBuf, ptr::{self, null_mut}, str::FromStr};
use futures::TryStreamExt;
use libc::{c_int, c_void};
use netlink_packet_route::{address::AddressAttribute, route::{RouteAddress, RouteAttribute, RouteMessage}};
use rdma_sys::*;
use log::info;
use rtnetlink::{new_connection, IpVersion};

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

pub unsafe fn send_complete(mut cq: *mut ibv_cq, channel: *mut ibv_comp_channel, iterations: usize, opcode_type: ibv_wc_opcode::Type) -> anyhow::Result<i32, CustomError>{
    let mut ret: c_int;
    //let empty_cq = ptr::null::<ibv_cq>() as *mut _;
    let mut context = ptr::null::<c_void>() as *mut _;

    let mut wc_vec: Vec<ibv_wc> = Vec::with_capacity(BATCH_SIZE);
    let wc_ptr = wc_vec.as_mut_ptr();

    let mut total_wc: i32 = 0;

    let nevents = 1;
    let solicited_only = 0;

    loop {
        ret = ibv_poll_cq(cq, BATCH_SIZE as i32, wc_ptr.wrapping_add(total_wc as usize));
        if ret < 0 {
            return Err(CustomError::new("ibv_poll_cq".to_string(), ret).into());
        }
        total_wc += ret;
        if total_wc >= iterations as i32{
            break;
        }
        ret = ibv_req_notify_cq(cq, solicited_only);
        if ret != 0 {
            return Err(CustomError::new("ibv_req_notify_cq".to_string(), ret).into());
        }
        ret = ibv_poll_cq(cq, BATCH_SIZE as i32, wc_ptr.wrapping_add(total_wc as usize));
        if ret < 0 {
            return Err(CustomError::new("ibv_poll_cq".to_string(), ret).into());
        }
        total_wc += ret;
        if total_wc >= iterations as i32{
            break;
        }
        ret = ibv_get_cq_event(channel, &mut cq, &mut context);
        if ret != 0 {
            return Err(CustomError::new("ibv_get_cq_event".to_string(), ret).into());
        }
        //assert!(cq == empty_cq && context as *mut rdma_cm_id == id.0);
        //assert!(cq == empty_cq);
        ibv_ack_cq_events(cq, nevents);
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
pub struct MemoryRegion(pub *mut ibv_mr);
unsafe impl Send for MemoryRegion{}
unsafe impl Sync for MemoryRegion{}
impl MemoryRegion{
    pub fn mr(&self) -> *mut ibv_mr{
        self.0
    }
}

#[derive(Clone)]
pub struct CompletionQueue(pub *mut ibv_cq);
unsafe impl Send for CompletionQueue{}
unsafe impl Sync for CompletionQueue{}
impl CompletionQueue{
    pub fn cq(&self) -> *mut ibv_cq{
        self.0
    }
}

#[derive(Clone)]
pub struct IbvGid(pub *mut ibv_gid);
unsafe impl Send for IbvGid{}
unsafe impl Sync for IbvGid{}
impl IbvGid{
    pub fn ibv_gid(&self) -> *mut ibv_gid{
        self.0
    }
}

#[derive(Clone)]
pub struct IbvSge(pub ibv_sge);
unsafe impl Send for IbvSge{}
unsafe impl Sync for IbvSge{}
impl IbvSge{
    pub fn sge(&self) -> ibv_sge{
        self.0
    }
}

#[derive(Clone)]
pub struct QueuePair(pub *mut ibv_qp);
unsafe impl Send for QueuePair{}
unsafe impl Sync for QueuePair{}
impl QueuePair{
    pub fn qp(&self) -> *mut ibv_qp{
        self.0
    }
}

#[derive(Clone)]
pub struct IbvContext(pub *mut ibv_context);
unsafe impl Send for IbvContext{}
unsafe impl Sync for IbvContext{}
impl IbvContext{
    pub fn ibv_context(&self) -> *mut ibv_context{
        self.0
    }
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

#[derive(Clone)]
pub struct ProtectionDomain(pub *mut ibv_pd);
unsafe impl Send for ProtectionDomain{}
unsafe impl Sync for ProtectionDomain{}
impl ProtectionDomain{
    pub fn pd(&self) -> *mut ibv_pd{
        self.0
    }
}

#[derive(Clone)]
pub struct EventChannel(pub *mut ibv_comp_channel);
unsafe impl Send for EventChannel{}
unsafe impl Sync for EventChannel{}
impl EventChannel{
    pub fn event_channel(&self) -> *mut ibv_comp_channel{
        self.0
    }
}

#[derive(Clone)]
pub struct Address(pub *mut c_void);
unsafe impl Send for Address{}
unsafe impl Sync for Address{}
impl Address{
    pub fn addr(&self) -> *mut c_void{
        self.0
    }
}

#[derive(Clone, Debug)]
pub struct Mr(pub *mut ibv_mr);
unsafe impl Send for Mr{}
unsafe impl Sync for Mr{}
impl Mr{
    pub fn mr(&self) -> *mut ibv_mr{
        self.0
    }
}

#[derive(Clone)]
pub struct InitAttr(pub ibv_qp_init_attr);
unsafe impl Send for InitAttr{}
unsafe impl Sync for InitAttr{}
impl InitAttr{
    pub fn init_attr(&self) -> ibv_qp_init_attr{
        self.0
    }
}


pub struct IbvSendWr(pub *mut ibv_send_wr);
unsafe impl Send for IbvSendWr{}
unsafe impl Sync for IbvSendWr{}
impl IbvSendWr{
    pub fn send_wr(&self) -> *mut ibv_send_wr{
        self.0
    }
}


pub struct IdWr{
    pub id: Id,
    pub wr_list: Vec<IbvSendWr>,
}

impl IdWr{
    pub fn new(id: Id) -> IdWr{
        IdWr{
            id,
            wr_list: Vec::new(),
        }
    }
    pub fn add_wr(&mut self, wr: IbvSendWr){
        self.wr_list.push(wr);
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
    mr: Mr,
}

impl Data{
    pub fn new(size: usize) -> Data{
        Data{
            buffer: vec![1u8; size],
            mr: Mr(null_mut()),
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
    fn set_mr(&mut self, mr: Mr){
        self.mr = mr;
    }
    fn mr(&self) -> Mr{
        self.mr.clone()
    }
}

#[derive(Clone)]
pub struct MrAddr{
    pub mr: *mut ibv_mr,
    pub addr: *mut c_void,
}

pub trait MrObject{
    fn mr_rkey(&self) -> u32{
        unsafe { (*self.mr().mr()).rkey as u32 }
    }
    fn mr_addr(&self) -> u64{
        unsafe { (*self.mr().mr()).addr as u64 }
    }
    fn len(&self) -> usize;
    fn addr(&mut self) -> *mut c_void;
    fn set_mr(&mut self, mr: Mr);
    fn mr(&self) -> Mr;
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
        self.set_mr(Mr(mr));
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
            unsafe { rdma_dereg_mr(self.mr().mr()) };
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
}

pub fn register_mr(id: &Id, obj: &mut dyn MrObject, operation: Operation) -> anyhow::Result<MrAddr, CustomError>{
    if id.id().is_null(){
        return Err(CustomError::new("id is null".to_string(), -1).into());
    }
    let length = obj.len();
    let addr: *mut c_void = obj.addr();
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
    Ok(MrAddr{mr, addr})
}



pub enum Operation{
    SendRecv,
    Write,
    Read,
}

#[derive(Debug, Clone)]
pub struct MetaData{
    pub request_type: u8,
    pub remote_address: u64,
    pub message_size: u64,
    pub rkey: u32,
    pub lkey: u32,
    pub iterations: u32,
    mr: Mr,
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
            mr: Mr(null_mut()),
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
    pub fn set_message_size(&mut self, length: u64){
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
    pub fn message_size(&self) -> u64{
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
    fn set_mr(&mut self, mr: Mr){
        self.mr = mr;
    }
    fn mr(&self) -> Mr{
        self.mr.clone()
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



/*
pub fn rdma_write(
    id: &Id,
    mr: Mr,
    addr: Address,
    rkey: u32,
    remote_addr: u64,
    mut offset: usize,
    msg_len: usize,
    messages_per_qp: u64,
) -> anyhow::Result<(), CustomError>{
    let mut flags = 0;
    let mut comp = false;

    for msg in 1..messages_per_qp+1{
        if msg == messages_per_qp || msg as usize % BATCH_SIZE == 0{
            flags = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0 | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0 | ibv_send_flags::IBV_SEND_SIGNALED.0;
            comp = true;
        }
        let addr = addr.addr().wrapping_add(offset);
        let remote_addr = remote_addr.wrapping_add(offset as u64);
        
        let ret = unsafe {
            rdma_post_write(
                id.0,
                null_mut(),
                addr,
                msg_len,
                mr.0,
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
            flags = 0;
            comp = false;
        }
        offset += msg_len;
    }
    Ok(())
}
    */

pub fn create_context(device_name: &CStr) -> *mut ibv_context {
    let device_list: *mut *mut ibv_device = unsafe { ibv_get_device_list(null_mut()) };
    if device_list.is_null() {
        return null_mut();
    }
    let mut device: *mut ibv_device = null_mut();
    let mut context: *mut ibv_context = null_mut();
    let mut found = false;
    let mut i = 0;
    while !device_list.is_null() {
        device = unsafe { *device_list.wrapping_add(i) };
        if device.is_null() {
            break;
        }
        let name: &CStr = unsafe { CStr::from_ptr((*device).name.as_ptr()) }; // Convert array to raw pointer
        if name == device_name {
            found = true;
            break;
        }
        i += 1;
    }
    if found {
        context = unsafe { ibv_open_device(device) };
    }
    unsafe { ibv_free_device_list(device_list) };
    context
}

pub fn create_event_channel(context: *mut ibv_context) -> anyhow::Result<EventChannel, CustomError> {
    let channel = unsafe { ibv_create_comp_channel(context) };
    if channel.is_null() {
        return Err(CustomError::new("ibv_create_comp_channel".to_string(), -1).into());
    }
    Ok(EventChannel(channel))
}

pub fn create_protection_domain(context: *mut ibv_context) -> anyhow::Result<ProtectionDomain, CustomError> {
    let pd = unsafe { ibv_alloc_pd(context) };
    if pd == null_mut() {
        return Err(CustomError::new("ibv_alloc_pd".to_string(), -1).into());
    }
    Ok(ProtectionDomain(pd))
}

pub fn create_create_completion_queue(context: *mut ibv_context, channel: *mut ibv_comp_channel) -> *mut ibv_cq {
    unsafe { ibv_create_cq(context, 4096, null_mut(), channel, 0) }
}

pub fn create_queue_pair(
    pd: *mut ibv_pd,
    cq: *mut ibv_cq,
) -> *mut ibv_qp {
    let mut qp_init_attr = ibv_qp_init_attr {
        qp_context: null_mut(),
        send_cq: cq,
        recv_cq: cq,
        srq: null_mut(),
        cap: ibv_qp_cap {
            max_send_wr: 4096,
            max_recv_wr: 4096,
            max_send_sge: 15,
            max_recv_sge: 15,
            max_inline_data: 64,
        },
        qp_type: ibv_qp_type::IBV_QPT_RC,
        sq_sig_all: 0,
    };
    unsafe { ibv_create_qp(pd, &mut qp_init_attr) }
}

pub fn set_qp_init_state(qp: *mut ibv_qp, port: u8) -> i32 {
    let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    qp_attr.qp_state = ibv_qp_state::IBV_QPS_INIT;
    qp_attr.pkey_index = 0;
    qp_attr.port_num = port;
    qp_attr.qp_access_flags = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0 | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0;
    let qp_attr_mask = ibv_qp_attr_mask::IBV_QP_STATE | ibv_qp_attr_mask::IBV_QP_PKEY_INDEX | ibv_qp_attr_mask::IBV_QP_PORT | ibv_qp_attr_mask::IBV_QP_ACCESS_FLAGS;
    unsafe { ibv_modify_qp(qp, &mut qp_attr, qp_attr_mask.0 as i32) }
}

pub fn connect_qp(qp: *mut ibv_qp, gid: ibv_gid, qpn: u32, psn: u32, my_psn: u32, gidx: i32) -> anyhow::Result<(), CustomError> {
    let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTR;
    qp_attr.path_mtu = ibv_mtu::IBV_MTU_4096;
    qp_attr.dest_qp_num = qpn;
    qp_attr.rq_psn = psn;
    qp_attr.max_dest_rd_atomic = 1;
    qp_attr.min_rnr_timer = 12;

    qp_attr.ah_attr.sl = 0;
    qp_attr.ah_attr.src_path_bits = 0;
    qp_attr.ah_attr.port_num = 1;
    qp_attr.ah_attr.dlid = 0;
    qp_attr.ah_attr.static_rate = 2;
    qp_attr.ah_attr.grh.dgid = gid;
    qp_attr.ah_attr.is_global = 1;
    qp_attr.ah_attr.grh.sgid_index = gidx as u8;
    qp_attr.ah_attr.grh.hop_limit = 10;

    let qp_attr_mask = 
        ibv_qp_attr_mask::IBV_QP_STATE |
        ibv_qp_attr_mask::IBV_QP_AV |
        ibv_qp_attr_mask::IBV_QP_PATH_MTU |
        ibv_qp_attr_mask::IBV_QP_DEST_QPN |
        ibv_qp_attr_mask::IBV_QP_RQ_PSN |
        ibv_qp_attr_mask::IBV_QP_MAX_DEST_RD_ATOMIC |
        ibv_qp_attr_mask::IBV_QP_MIN_RNR_TIMER;
    let ret = unsafe { ibv_modify_qp(qp, &mut qp_attr, qp_attr_mask.0 as i32) };
    if ret != 0 {
        println!("remote gid: {}", gid_to_ipv6_string(gid).unwrap());
        println!("qp_attr.dest_qp_num: {}", qp_attr.dest_qp_num);
        println!("qp_attr.rq_psn: {}", qp_attr.rq_psn);
        println!("qp_attr.ah_attr.dlid: {}", qp_attr.ah_attr.dlid);
        println!("qp_attr.ah_attr.grh.dgid.gid.global.subnet_prefix: {}", unsafe { gid.global.subnet_prefix });
        println!("qp_attr.ah_attr.grh.dgid.gid.global.interface_id: {}", unsafe { gid.global.interface_id });
        println!("qp_attr.ah_attr.grh.sgid_index: {}", qp_attr.ah_attr.grh.sgid_index);
        return Err(CustomError::new("ibv_modify_qp to rtr ".to_string(), ret).into());
    }

    qp_attr.qp_state = ibv_qp_state::IBV_QPS_RTS;
    qp_attr.timeout = 14;
    qp_attr.retry_cnt = 7;
    qp_attr.rnr_retry = 7;
    qp_attr.sq_psn = my_psn;
    qp_attr.max_rd_atomic = 1;
    let qp_attr_mask = 
        ibv_qp_attr_mask::IBV_QP_STATE |
        ibv_qp_attr_mask::IBV_QP_TIMEOUT |
        ibv_qp_attr_mask::IBV_QP_RETRY_CNT |
        ibv_qp_attr_mask::IBV_QP_RNR_RETRY |
        ibv_qp_attr_mask::IBV_QP_SQ_PSN |
        ibv_qp_attr_mask::IBV_QP_MAX_QP_RD_ATOMIC;

    let ret = unsafe { ibv_modify_qp(qp, &mut qp_attr, qp_attr_mask.0 as i32) };
    if ret != 0 {
        return Err(CustomError::new("ibv_modify_qp to rts ".to_string(), ret).into());
    }
    Ok(())
}

pub fn get_gid(context: *mut ibv_context, port_num: u8) -> ibv_gid {
    let mut gid: ibv_gid = unsafe { std::mem::zeroed() };
    unsafe { ibv_query_gid(context, port_num, 1, &mut gid) };
    gid
}

pub fn get_port_info(context: *mut ibv_context, port_num: u8) -> anyhow::Result<ibv_port_attr, CustomError> {
    let mut port_attr: ibv_port_attr = unsafe { std::mem::zeroed() };
    let ret = unsafe { ___ibv_query_port(context, port_num, &mut port_attr) };
    if ret != 0 {
        return Err(CustomError::new("ibv_query_port".to_string(), ret).into());
    }
    Ok(port_attr)
}

pub fn gen_psn() -> u32{
    rand::random::<u32>() & 0xffffff
}

pub struct Destination{
    pub lid: u16,
    pub qpn: u32,
    pub psn: u32,
    pub gid: ibv_gid,
}

impl Destination {
    pub fn gid_to_ipv6_string(&self) -> String {
        unsafe {
            // Access the raw bytes of the gid union
            let raw_gid = self.gid.raw;

            // Create an Ipv6Addr from the raw bytes
            let ipv6_addr = Ipv6Addr::new(
                (raw_gid[0] as u16) << 8 | (raw_gid[1] as u16),
                (raw_gid[2] as u16) << 8 | (raw_gid[3] as u16),
                (raw_gid[4] as u16) << 8 | (raw_gid[5] as u16),
                (raw_gid[6] as u16) << 8 | (raw_gid[7] as u16),
                (raw_gid[8] as u16) << 8 | (raw_gid[9] as u16),
                (raw_gid[10] as u16) << 8 | (raw_gid[11] as u16),
                (raw_gid[12] as u16) << 8 | (raw_gid[13] as u16),
                (raw_gid[14] as u16) << 8 | (raw_gid[15] as u16),
            );

            // Convert the Ipv6Addr to a string
            ipv6_addr.to_string()
        }
    }
}

pub fn gid_to_ipv6_string(gid: ibv_gid) -> Option<Ipv6Addr> {
    unsafe {
        // Access the raw bytes of the gid union
        let raw_gid = gid.raw;
        // check if all bytes are zero
        let mut all_zero = true;
        for i in 0..16{
            if raw_gid[i] != 0{
                all_zero = false;
                break;
            }
        }
        if all_zero{
            return None;
        }

        // Create an Ipv6Addr from the raw bytes
        let ipv6_addr = Ipv6Addr::new(
            (raw_gid[0] as u16) << 8 | (raw_gid[1] as u16),
            (raw_gid[2] as u16) << 8 | (raw_gid[3] as u16),
            (raw_gid[4] as u16) << 8 | (raw_gid[5] as u16),
            (raw_gid[6] as u16) << 8 | (raw_gid[7] as u16),
            (raw_gid[8] as u16) << 8 | (raw_gid[9] as u16),
            (raw_gid[10] as u16) << 8 | (raw_gid[11] as u16),
            (raw_gid[12] as u16) << 8 | (raw_gid[13] as u16),
            (raw_gid[14] as u16) << 8 | (raw_gid[15] as u16),
        );

        // Convert the Ipv6Addr to a string
        Some(ipv6_addr)
    }
}

pub fn get_gid_ctx_port_from_v4_ip(ip: &Ipv4Addr) -> anyhow::Result<(IbvGid, IbvContext, u8, i32), CustomError> {
    let device_list: *mut *mut ibv_device = unsafe { __ibv_get_device_list(null_mut()) };

    // get the number of elements in the list
    let mut num_devices = 0;
    while !unsafe { *device_list.offset(num_devices) }.is_null() {
        num_devices += 1;
    }
    if num_devices == 0 {
        return Err(CustomError::new("ibv_get_device_list".to_string(), -1).into());
    }
    for i in 0..num_devices {
        let device = unsafe { *device_list.offset(i as isize) };
        let device_ctx = unsafe { ibv_open_device(device) };
        if device_ctx == null_mut() {
            return Err(CustomError::new("Failed to open device".to_string(), -1));
        }
        let dervice_name = unsafe { CStr::from_ptr((*device).name.as_ptr()) };
        let mut device_attr = unsafe { std::mem::zeroed::<ibv_device_attr>() };
        let ret = unsafe { ibv_query_device(device_ctx, &mut device_attr) };
        if ret != 0 {
            return Err(CustomError::new("Failed to query device".to_string(), ret));
        }
        let num_ports = device_attr.phys_port_cnt;
        for i in 1..=num_ports {
            let mut port_attr = unsafe { std::mem::zeroed::<ibv_port_attr>() };
            let ret = unsafe { ___ibv_query_port(device_ctx, i, &mut port_attr) };
            if ret != 0 {
                return Err(CustomError::new("Failed to query port".to_string(), ret));
            }
            let gid_tbl_len = port_attr.gid_tbl_len;
            for j in 0..gid_tbl_len {
                let mut gid: ibv_gid = unsafe { std::mem::zeroed() };
                unsafe { ibv_query_gid(device_ctx, i, j, &mut gid) };
                if let Some(gid_v6) = gid_to_ipv6_string(gid){
                    if let Some(gid_v4) = gid_v6.to_ipv4(){
                        if gid_v4 == ip.clone(){
                            // read qid type from /sys/class/infiniband/{device_name}/ports/{i}/gid_attrs/types/{j}
                            match read_gid_type(dervice_name.to_str().unwrap(), i, j)?{
                                GidType::ROCEv2 => {
                                    let gid = Box::new(gid);
                                    let gid_ptr = Box::into_raw(gid);
                                    let device_ctx: Box<ibv_context> = unsafe { Box::from_raw(device_ctx) };
                                    let device_ctx_ptr = Box::into_raw(device_ctx);
                                    return Ok((IbvGid(gid_ptr), IbvContext(device_ctx_ptr), i, j));
                                },
                                GidType::RoCEv1 => {

                                },
                            }

                        }
                    }
                }
            }
        }

    }
    return Err(CustomError::new("get_gid_from_v4_ip".to_string(), -1).into());

}

enum GidType{
    ROCEv2,
    RoCEv1,
}
impl GidType{
    fn from_str(s: &str) -> GidType{
        match s{
            "RoCE v2" => GidType::ROCEv2,
            "IB/RoCE v1" => GidType::RoCEv1,
            _ => GidType::ROCEv2,
        }
    }
}

fn read_gid_type(device_name: &str, port: u8, gid_index: i32) -> anyhow::Result<GidType, CustomError> {
    // Construct the file path
    let path = PathBuf::from(format!(
        "/sys/class/infiniband/{}/ports/{}/gid_attrs/types/{}",
        device_name, port, gid_index
    ));

    // Read the file contents
    let gid_type = fs::read_to_string(path).map_err(|e| CustomError::new(e.to_string(), -1))?;

    // Return the contents as a String
    let gid_type = GidType::from_str(gid_type.trim());
    Ok(gid_type)
}

pub async fn route_lookup(dst_ip: String) -> anyhow::Result<Ipv4Addr, CustomError>{
    let dst_ip = Ipv4Addr::from_str(&dst_ip).unwrap();
    let (connection, handle, _) = new_connection().unwrap();
    tokio::spawn(connection);
    let mut route_table = vec![BTreeMap::<u32, RouteMessage>::new(); 33];
    let mut route_messages = handle.route().get(IpVersion::V4).execute();
    while let Some(route_message) = route_messages.try_next().await.unwrap() {
        let prefix_len = route_message.header.destination_prefix_length;
        for route_attr in &route_message.attributes {
            if let RouteAttribute::Destination(route_address) = route_attr {
                if let RouteAddress::Inet(prefix) = route_address {
                    let prefix = u32::from_be_bytes(prefix.octets());
                    route_table[prefix_len as usize].insert(prefix, route_message.clone());
                }
            }
        }
    }
    let dst_ip_dec = u32::from_be_bytes(dst_ip.octets());
    for (prefix_len, prefixes) in route_table.iter().enumerate().rev(){
        let mask: u32 = 0xffffffff << (32 - prefix_len);
        let masked_dst_ip = dst_ip_dec & mask;
        if let Some(route_message) = prefixes.get(&masked_dst_ip){
            for route_attr in &route_message.attributes {
                if let RouteAttribute::PrefSource(route_address) = route_attr {
                    if let RouteAddress::Inet(pref_source) = route_address {
                        info!("local ip: {}", pref_source);
                        return Ok(pref_source.clone());
                    }
                }
            }
            for route_attr in &route_message.attributes {
                if let RouteAttribute::Oif(oif) = route_attr {
                    // get the interface from the oif index
                    while let Some(address_message) = handle.address().get().set_link_index_filter(*oif).execute().try_next().await.unwrap() {
                        for address_attr in &address_message.attributes {
                            if let AddressAttribute::Local(ip_address) = address_attr {
                                match ip_address {
                                    std::net::IpAddr::V4(ip) => {
                                        info!("local ip: {}", ip);
                                        return Ok(ip.clone());
                                    },
                                    _ => {}
                                }
                            }
                        }
                        return Err(CustomError::new("ip address not found".to_string(), -1).into());
                    }
                }
            }
        }
    }
    Err(CustomError::new("route not found".to_string(), -1).into())
}