use clap::Parser;
use common::CustomError;
use grpc_server::GrpcServer;
use server_manager::ServerManager;
use rdma_server::RdmaServer;

pub mod grpc_server;
pub mod connection_manager;
pub mod rdma_server;
pub mod server_manager;

#[derive(Parser)]
struct Args{
    #[clap(short, long)]
    address: String,
    #[clap(short, long)]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<(), CustomError> {

    let args = Args::parse();

    let mut jh_list = Vec::new();

    let rdma_server = RdmaServer::new();
    let rdma_server_client = rdma_server.client.clone();
    let jh = tokio::spawn(async move{
        rdma_server.run().await.unwrap();
    });
    jh_list.push(jh);

    let sm = ServerManager::new(args.address.clone(), rdma_server_client);
    let sm_client = sm.client.clone();
    let jh = tokio::spawn(async move{
        sm.run().await;
    });
    jh_list.push(jh);

    let grpc_address = format!("{}:{}",args.address,args.port);
    let jh = tokio::spawn(async move{
        let grpc_server = GrpcServer::new(grpc_address, sm_client);
        grpc_server.run().await.unwrap();
    });
    jh_list.push(jh);


    futures::future::join_all(jh_list).await;





    //let mut rdma_server = RdmaServer::new();
    //rdma_server.listen(address, port)?;
    //rdma_server.write()?;


    Ok(())
}