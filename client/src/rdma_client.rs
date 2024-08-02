use std::{collections::HashMap, ptr::null_mut, sync::{Arc, Mutex}};
use common::*;
use rdma_sys::*;

const BATCH_SIZE: u64 = 10;

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
        let qps_to_use = if messages > self.id.len() as u64 { self.id.len() as u64 } else { messages };


        /*
        for (i, queue) in queues.iter().enumerate() {
            println!("Queue pair {}: {:#?}", i + 1, queue);
        }
        */

        //let messages_per_qp = messages / qps_to_use;
        let mut data_list = Vec::with_capacity(qps_to_use as usize);
        let mut metadata_mr_map = HashMap::new();
        let mut data = Data::new(volume as usize);
        let data_addr = data.addr();
        let data_len = data.len();
        let mut rkey = None;
        let mut remote_address = None;

        let mut queues = distribute_data(volume, qps_to_use, message_size, BATCH_SIZE as u64, iterations as u64);
        consolidate_blocks(&mut queues, BATCH_SIZE);
        
        for (qp_idx, _queue) in queues.iter().enumerate() {
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

        for (qp_idx, queue) in queues.iter_mut().enumerate() {
            let (mr_addr, _id, _metadata_request) = data_list.get_mut(qp_idx).unwrap();
            for message_block in &mut queue.message_blocks{
                for message in &mut message_block.messages{
                    let addr = unsafe { (*mr_addr.mr).addr };
                    message.sge.addr = addr.wrapping_add(message.offset as usize) as u64;
                    message.sge.length = message.size as u32;
                    message.sge.lkey = unsafe { (*mr_addr.mr).lkey };
                }
            }
        }
        
        //let mut id_wr_list = Vec::new();
        //let mut qp_list = Vec::new();



        let mut qp_map = HashMap::new();
        for qp_idx in 0..qps_to_use{
            qp_map.insert(qp_idx, Vec::new());
        }
    


        let mut completion_map = HashMap::new();
        for qp_idx in 0..qps_to_use{
            completion_map.insert(qp_idx, (Id(null_mut()), 0));
        }

        let mut id_map = HashMap::new();
        let mut qp_idwr_map: HashMap<usize, IdWr> = HashMap::new();
        let mut total_msg_size = 0;
        for (qp_idx, queue) in queues.iter_mut().enumerate() {
            let (_mr_addr, id, metadata_request) = data_list.get_mut(qp_idx as usize).unwrap();
            id_map.insert(qp_idx, id.clone());
            for message_block in &mut queue.message_blocks{
                let message_block_len = message_block.messages.len();
                for msg in &mut message_block.messages{
                    total_msg_size += msg.size;
                    let mut id_wr = qp_idwr_map.remove(&qp_idx).unwrap_or_else(|| IdWr::new(id.clone()));
                    let sge = &mut msg.sge;
                    let mut wr = unsafe { std::mem::zeroed::<ibv_send_wr>() };
                    wr.opcode = ibv_wr_opcode::IBV_WR_RDMA_WRITE;
                    wr.sg_list = sge;
                    wr.num_sge = 1;
                    let remote_addr = metadata_request.remote_address();
                    let remote_addr = remote_addr.wrapping_add(msg.offset);
                    wr.wr.rdma.remote_addr = remote_addr;
                    wr.wr.rdma.rkey = metadata_request.rkey();
                    let current_msg_count = id_wr.wr_list.len() + 1;
                    if current_msg_count == message_block_len {
                        wr.send_flags =  ibv_send_flags::IBV_SEND_SIGNALED.0;
                    } else {
                        wr.send_flags = 0;
                    }
                    id_wr.add_wr(IbvSendWr(wr));
                    if current_msg_count == message_block_len {
                        id_wr.set_wr_ptr_list();
                        qp_map.get_mut(&(qp_idx as u64)).unwrap().push(id_wr);
                        id_wr = IdWr::new(id.clone());
                    }
                    qp_idwr_map.insert(qp_idx, id_wr);
                }
            }
        }
        

        

        let mut bla: u64 = 0;
        for (_qp_idx, id_wr_list) in &qp_map{
            for (_idwr_idx, id_wr) in id_wr_list.iter().enumerate(){
                let wr = &id_wr.wr_list.get(0).unwrap().0;
                print_wr_ids(wr);

                for (_wr_idx, wr) in id_wr.wr_list.iter().enumerate(){
                    let sge = &wr.0.sg_list;
                    bla += unsafe { (**sge).length as u64 };
                }
            }
        }

        println!("total_msg_size: {}, volume: {}, bla: {}", total_msg_size, volume, bla);
        if bla as u64 != total_msg_size{
            panic!("bla {} != total_msg_size {}", bla, total_msg_size);
        }
        
            let mut bla = 0;
            for (qp_idx, id_wr_list) in &qp_map{
                println!("qp_idx: {}, id_wr_list: {}", qp_idx, id_wr_list.len());
                for (idwr_idx, id_wr) in id_wr_list.iter().enumerate(){
                    println!("idwr_idx: {}, wrs: {}", idwr_idx, id_wr.wr_list.len());
                    for (wr_idx, wr) in id_wr.wr_list.iter().enumerate(){
                        let sge = &wr.0.sg_list;
                        let remote_addr = unsafe { wr.0.wr.rdma.remote_addr };
                        println!("wr_idx: {}, addr: {}, len: {}, remote_addr: {}", wr_idx, unsafe { (**sge).addr }, unsafe { (**sge).length }, remote_addr);
                        bla += unsafe { (**sge).length };
                    }
                }
            }
            println!("total_msg_size: {}, volume: {}, bla: {}", total_msg_size, volume, bla);
        


        

        


        let mut jh_list = Vec::new();
        let iter_now = tokio::time::Instant::now();
        for (qp_idx, id_wr_list) in qp_map.drain(){
            let id_wr_list = Arc::new(Mutex::new(id_wr_list));
            //let random_number = rand::random::<u8>() % 200;
            
            let id_wr_list = id_wr_list.clone();
            let jh = tokio::spawn(async move {
                tokio::time::sleep(tokio::time::Duration::from_millis(qp_idx * 10)).await;
                let mut id_wr_list = id_wr_list.lock().unwrap();
                while let Some(mut id_wr) = id_wr_list.pop(){
                    let first_wr = &mut id_wr.wr_list.get_mut(0).unwrap().0;
                    println!("qp_idx: {}, id_wr_list: {}", qp_idx, id_wr_list.len());
                    print_wr_ids(&first_wr);
                    let id = id_wr.id.clone();
                    let qp = unsafe { (*id.id()).qp };
                    let mut bad_wr = null_mut();
                    let ret = unsafe { ibv_post_send(qp, &mut *first_wr, &mut bad_wr) };
                    if ret != 0 {
                        println!("ibv_post_send failed");
                    }
                    let _compl = unsafe { send_complete(id, 1, ibv_wc_opcode::IBV_WC_RDMA_WRITE) }.unwrap();
                    //println!("qp_idx: {}, compl: {}", _qp_idx, compl);
                }
            });
            jh_list.push(jh);
        }
        futures::future::join_all(jh_list).await;
        
        //futures::future::join_all(jh_list).await;
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

pub fn print_wr_ids(start: &ibv_send_wr) {
    let mut current = start;


    while !current.next.is_null() {
        let sge = &current.sg_list;
        let sge_len = unsafe { (**sge).length as u64 };
        let sge_addr = unsafe { (**sge).addr };
        let remote_addr = unsafe { current.wr.rdma.remote_addr };
        println!("wr_id: {}, sge_addr: {}, sge_len: {}, remote_addr: {}", current.wr_id, sge_addr, sge_len, remote_addr);

        unsafe {
            // Move to the next element
            current = &*current.next;
        }

    }
    let sge = &current.sg_list;
    let sge_len = unsafe { (**sge).length as u64 };
    let sge_addr = unsafe { (**sge).addr };
    let remote_addr = unsafe { current.wr.rdma.remote_addr };
    println!("wr_id: {}, sge_addr: {}, sge_len: {}, remote_addr: {}", current.wr_id, sge_addr, sge_len, remote_addr);
}

#[derive(Clone)]
struct QueuePair {
    message_blocks: Vec<MessageBlock>,
}

#[derive(Clone)]
struct MessageBlock {
    messages: Vec<Message>,
}

#[derive(Clone)]
struct Message {
    sge: ibv_sge,
    offset: u64,
    size: u64,
}

fn distribute_data(volume: u64, queue_pairs: u64, max_message_size: u64, max_msg_per_block: u64, iterator_factor: u64) -> Vec<QueuePair> {
    // Calculate the base size for each queue pair
    let base_size = volume / queue_pairs;
    let remainder = volume % queue_pairs;

    // Initialize queues
    let mut queues: Vec<QueuePair> = vec![QueuePair { message_blocks: Vec::new() }; queue_pairs as usize];

    for factor in 0..iterator_factor {
        let mut current_offset = factor * volume;

        for i in 0..queue_pairs {
            let mut size = base_size;
            if i < remainder {
                size += 1; // Distribute the remainder
            }

            let mut current_block = MessageBlock { messages: Vec::new() };
            let mut current_block_size = 0;

            while size > 0 {
                let message_size = if size > max_message_size { max_message_size } else { size };

                current_block.messages.push(Message {
                    offset: current_offset % volume,
                    size: message_size,
                    sge: unsafe { std::mem::zeroed::<ibv_sge>() }
                });

                current_offset = (current_offset + message_size) % volume;
                current_block_size += 1;
                size -= message_size;

                if current_block_size >= max_msg_per_block {
                    queues[i as usize].message_blocks.push(current_block);
                    current_block = MessageBlock { messages: Vec::new() };
                    current_block_size = 0;
                }
            }

            if !current_block.messages.is_empty() {
                queues[i as usize].message_blocks.push(current_block);
            }
        }
    }

    queues
}

fn consolidate_blocks(queues: &mut Vec<QueuePair>, max_msg_per_block: u64) {
    for queue in queues.iter_mut() {
        let mut new_blocks = Vec::new();
        let mut current_block = MessageBlock { messages: Vec::new() };

        for block in &queue.message_blocks {
            for message in &block.messages {
                if current_block.messages.len() as u64 >= max_msg_per_block {
                    new_blocks.push(current_block);
                    current_block = MessageBlock { messages: Vec::new() };
                }
                current_block.messages.push(message.clone());
            }
        }

        if !current_block.messages.is_empty() {
            new_blocks.push(current_block);
        }

        queue.message_blocks = new_blocks;
    }
}

fn aggregate_total_size(queues: &Vec<QueuePair>) -> u64 {
    queues.iter().flat_map(|queue| &queue.message_blocks)
          .flat_map(|block| &block.messages)
          .map(|message| message.size)
          .sum()
}