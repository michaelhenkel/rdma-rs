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

pub fn register_send_recv_mr(id: *mut rdma_cm_id, addr: *mut c_void, length: usize) -> anyhow::Result<*mut ibv_mr, CustomError>{
    if id.is_null(){
        return Err(CustomError::new("id is null".to_string(), -1).into());
    }
    let mr = unsafe { rdma_reg_msgs(id, addr, length) };
    if mr.is_null() {
        unsafe { rdma_dereg_mr(mr); }
        return Err(CustomError::new("rdma_reg_msgs".to_string(), -1).into());
    }
    Ok(mr)
}

pub fn register_write_mr(id: *mut rdma_cm_id, addr: *mut c_void, length: usize) -> anyhow::Result<*mut ibv_mr, CustomError>{
    let mr = unsafe { rdma_reg_write(id, addr, length) };
    if mr.is_null() {
        unsafe { rdma_dereg_mr(mr); }
        return Err(CustomError::new("rdma_reg_write".to_string(), -1).into());
    }
    Ok(mr)
}

pub fn register_read_mr(id: *mut rdma_cm_id, addr: *mut c_void, length: usize) -> anyhow::Result<*mut ibv_mr, CustomError>{
    let mr = unsafe { rdma_reg_read(id, addr, length) };
    if mr.is_null() {
        unsafe { rdma_dereg_mr(mr); }
        return Err(CustomError::new("rdma_reg_read".to_string(), -1).into());
    }
    Ok(mr)
}

pub fn rdma_recv_md(id: *mut rdma_cm_id, mr: *mut ibv_mr, addr: *mut c_void, length: usize) -> anyhow::Result<u32, CustomError>{
    let mut ret = unsafe { rdma_post_recv(id, null_mut(), addr, length, mr) };
    if ret != 0 {
        unsafe { rdma_dereg_mr(mr) };
        return Err(CustomError::new("rdma_post_recv".to_string(), ret).into());
    }
    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    while ret == 0 {
        ret = unsafe { rdma_get_recv_comp(id, &mut wc) };
    }
    if ret < 0 {
        unsafe { rdma_disconnect(id); }
        return Err(CustomError::new("rdma_get_recv_comp".to_string(), ret).into());
    }
    println!("opcode: {}", wc.opcode);
    println!("status: {}", wc.status);
    Ok(wc.opcode)
}

pub fn rdma_send_md(id: *mut rdma_cm_id, mr: *mut ibv_mr, addr: *mut c_void, length: usize) -> anyhow::Result<(), CustomError>{
    let flags = ibv_send_flags::IBV_SEND_INLINE.0 | ibv_send_flags::IBV_SEND_SIGNALED.0;
    let mut ret = unsafe {
        rdma_post_send(
            id,
            null_mut(),
            addr,
            length,
            mr,
            flags as i32,
        )
    };
    if ret != 0 {
        unsafe { rdma_disconnect(id) };
        return Err(CustomError::new("rdma_post_send".to_string(), ret).into());
    }
    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    while ret == 0 {
        ret = unsafe { rdma_get_send_comp(id, &mut wc) };
    }
    if ret < 0 {
        println!("rdma_get_send_comp");
    }
    Ok(())
}

pub fn rdma_recv_data(id: *mut rdma_cm_id, mr: *mut ibv_mr, addr: *mut c_void, length: usize, iterations: usize) -> anyhow::Result<(), CustomError>{
    let mut ret;
    let mut comp = false;
    for i in 1..iterations+1{
        if i == iterations || i % BATCH_SIZE == 0{
            comp = true;
        }
        ret = unsafe {
            rdma_post_recv(
                id,
                null_mut(),
                addr,
                length,
                mr
            )
        };
        if ret != 0 {
            unsafe { rdma_disconnect(id) };
            return Err(CustomError::new("rdma_post_write".to_string(), ret).into());
        }
    
        if comp{
            unsafe { recv_complete(MyId(id), i)? };
            comp = false;
        }
        
        
    }
    Ok(())
}

pub fn rdma_send_data(id: MyId, mr: MyMr, addr: MyAddress, length: usize, iterations: usize) -> anyhow::Result<(), CustomError>{
    let mut flags = 0;
    let mut ret;
    let mut completed = 0;
    let mut uncompleted = 0;
    let mut comp = false;
    for i in 1..iterations+1{
        if i == iterations || i % BATCH_SIZE == 0{
            flags = ibv_send_flags::IBV_SEND_SIGNALED.0;
            comp = true;
            uncompleted += 1;
        }
        ret = unsafe {
            rdma_post_send(
                id.0,
                null_mut(),
                addr.0,
                length,
                mr.0,
                flags as i32
            )
        };

        if ret != 0 {
            unsafe { rdma_disconnect(id.0) };
            return Err(CustomError::new("rdma_post_write".to_string(), ret).into());
        }
        if comp{
            let ret = unsafe { send_complete(id.clone(), 1)? };
            uncompleted -= ret;
            completed += ret;
            comp = false;
            flags = 0;
        }
    }
    println!("iterations: {} uncompleted: {}, completed {}", iterations, uncompleted, completed);
    Ok(())
}

pub unsafe fn send_complete(id: MyId, iterations: usize) -> anyhow::Result<i32, CustomError>{
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
        println!("total_wc: {}, iterations: {}", total_wc, iterations);
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
        println!("opcode: {}, status: {}", opcode, status);
        if status != ibv_wc_status::IBV_WC_SUCCESS || opcode != ibv_wc_opcode::IBV_WC_RDMA_WRITE{
            return Err(CustomError::new("wc status or opcode wrong".to_string(), -1).into());
        }
    }
    Ok(total_wc)
}

pub unsafe fn recv_complete(id: MyId, iterations: usize) -> anyhow::Result<i32, CustomError>{
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
        println!("total_wc: {}, iterations: {}", total_wc, iterations);
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
        let opcode = (*wc).opcode;
        println!("opcode: {}, status: {}", opcode, status);
        //if status != ibv_wc_status::IBV_WC_SUCCESS || opcode != ibv_wc_opcode::IBV_WC_RDMA_WRITE{
        //    return Err(CustomError::new("wc status or opcode wrong".to_string(), -1).into());
        //}
    }
    println!("total_wc: {}, iterations: {}", total_wc, iterations);
    Ok(total_wc)
}

pub fn rdma_write(id: MyId, mr: MyMr, addr: MyAddress, length: usize, rkey: u32, remote_addr: u64, iterations: usize) -> anyhow::Result<(), CustomError>{
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
                addr.0,
                length,
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
            let _ret = unsafe { send_complete(id.clone(), 1)? };
            comp = false;
            flags = 0;
        }
    }
    Ok(())
}

#[derive(Clone)]
pub struct MyId(pub *mut rdma_sys::rdma_cm_id);
unsafe impl Send for MyId{}
unsafe impl Sync for MyId{}

pub struct MyAddress(pub *mut c_void);
unsafe impl Send for MyAddress{}
unsafe impl Sync for MyAddress{}

pub struct MyMr(pub *mut ibv_mr);
unsafe impl Send for MyMr{}
unsafe impl Sync for MyMr{}


pub fn read(id: *mut rdma_cm_id, mr: *mut ibv_mr, addr: *mut c_void, length: usize, rkey: u32, remote_addr: u64) -> anyhow::Result<(), CustomError>{
    let flags: u32 = ibv_access_flags::IBV_ACCESS_LOCAL_WRITE.0 | ibv_access_flags::IBV_ACCESS_REMOTE_READ.0 | ibv_access_flags::IBV_ACCESS_REMOTE_WRITE.0;
    let mut ret = unsafe {
        rdma_post_read(
            id,
            null_mut(),
            addr,
            length,
            mr,
            flags as i32,
            remote_addr,
            rkey
        )
    };
    if ret != 0 {
        unsafe { rdma_disconnect(id) };
        return Err(CustomError::new("rdma_post_read".to_string(), ret).into());
    }
    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    while ret == 0 {
        ret = unsafe { rdma_get_send_comp(id, &mut wc) };
    }
    if ret < 0 {
        println!("rdma_get_send_comp");
    }
    Ok(())
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

#[derive(Default, Debug)]
pub struct MetaData{
    pub request_type: u8,
    pub address: u64,
    pub length: u32,
    pub rkey: u32,
    pub lkey: u32,
    pub iterations: u32,
}

impl MetaData{
    pub const LEN: usize = std::mem::size_of::<MetaData>();
}