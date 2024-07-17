use std::str::FromStr;

use clap::Parser;
use common::CustomError;
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
    #[clap(long, default_value = "128")]
    size: usize,
    #[clap(short, long, default_value = "5")]
    iterations: usize,
    #[clap(short, long, default_value = "1")]
    qps: u32,
    #[clap(short, long, default_value = "1MB")]
    volume: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), CustomError> {
    let args = Args::parse();
    let volume = byte_unit::Byte::from_str(args.volume.as_str()).map_err(|_| CustomError::new("wrong byte unit".to_string(), -1))?;
    let volume = volume.as_u64();
    let grpc_address = format!("http://{}:{}",args.server,args.port);
    let grpc_client = grpc_client::GrpcClient::new(grpc_address,0 );
    let mut rdma_client = RdmaClient::new();
    grpc_client.create_rdma_server().await.unwrap();
    let mut ports = Vec::new();
    for _ in 0..args.qps{
        let port = grpc_client.create_qp().await.unwrap();
        ports.push(port);
    }
    for port in &ports{
        grpc_client.request_qp_connection(port.clone()).await.unwrap();
    }

    let mut pd = None;

    for rdma_port in &ports{
        let port = format!("{}\0",rdma_port);
        let server = format!("{}\0",args.server);
        let port = port.as_str();
        let server = server.as_str();
        println!("Connecting to {}:{}",server,rdma_port);
        rdma_client.connect(server,port, &mut pd)?;
    }
    for port in ports{
        grpc_client.listen(port).await.unwrap();
    }


    //rdma_client.read(args.msg_size, args.iterations)?;
    //rdma_client.send(args.msg_size, args.iterations)?;
    println!("Client writing");
    rdma_client.write(args.size, args.iterations, volume)?;
    rdma_client.disconnect()?;
    println!("Client done");
    Ok(())
}
