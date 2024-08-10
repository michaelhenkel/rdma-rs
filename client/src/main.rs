use std::str::FromStr;

use clap::Parser;
use common::*;
use rdma_client::RdmaClient;
pub mod grpc_client;
pub mod connection_manager;
pub mod rdma_client;

#[derive(Parser)]
struct Args{
    #[clap(short, long)]
    server: String,
    #[clap(short, long, default_value = "7471")]
    port: u16,
    #[clap(short, long, default_value = "65536B")]
    msg_size: String,
    #[clap(short, long, default_value = "5")]
    iterations: usize,
    #[clap(short, long, default_value = "1")]
    qps: u32,
    #[clap(short, long, default_value = "1MB")]
    volume: String,
    #[clap(long, default_value = "1")]
    delay: u64,
}



#[tokio::main]
async fn main() -> anyhow::Result<(), CustomError> {
    let logger = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).build();
    let _ = logger;
    let args = Args::parse();
    let volume = byte_unit::Byte::from_str(args.volume.as_str()).map_err(|_| CustomError::new("wrong byte unit".to_string(), -1))?;
    let volume = volume.as_u64();
    let msg_size = byte_unit::Byte::from_str(args.msg_size.as_str()).map_err(|_| CustomError::new("wrong byte unit".to_string(), -1))?;
    let msg_size = msg_size.as_u64();
    let rdma_client = RdmaClient::new(args.server.clone(), args.port, args.qps as usize).await?;
    let (local_mr, remote_mr) = rdma_client.register_memory_region(volume).await?;
    rdma_client.write(local_mr, remote_mr, args.iterations, msg_size, args.delay).await?;
    println!("Client done");
    Ok(())
}
