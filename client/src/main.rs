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
    #[clap(short, long, default_value = "128")]
    msg_size: usize,
    #[clap(short, long, default_value = "5")]
    iterations: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), CustomError> {
    let args = Args::parse();
    let grpc_address = format!("http://{}:{}",args.server,args.port);
    let grpc_client = grpc_client::GrpcClient::new(grpc_address,0 );
    let rdma_port = grpc_client.request_connection().await.unwrap();
    let port = format!("{}\0",rdma_port);
    let server = format!("{}\0",args.server);
    let port = port.as_str();
    let server = server.as_str();
    let mut rdma_client = RdmaClient::new();
    rdma_client.connect(server,port)?;
    grpc_client.listen().await.unwrap();
    //rdma_client.send(args.msg_size, args.iterations)?;
    rdma_client.write(args.msg_size, args.iterations)?;
    rdma_client.disconnect()?;
    println!("client done");
    Ok(())
}
