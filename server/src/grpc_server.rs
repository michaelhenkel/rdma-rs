use crate::{
    connection_manager::connection_manager::{
    connection_manager_server::{
        ConnectionManager,
        ConnectionManagerServer
    }, MemoryRegionRequest, MemoryRegionResponse, QueuePairRequest, QueuePairResponse, RdmaServerRequest, StatusResponse
},
server_manager::ServerManagerClient};
use tonic::{transport::Server, Request, Response, Status};


#[derive(Clone)]
pub struct GrpcServer{
    address: String,
    server_manager_client: ServerManagerClient,
}

impl GrpcServer {
    pub fn new(address: String, server_manager_client: ServerManagerClient) -> GrpcServer {
        GrpcServer {
            address,
            server_manager_client,
        }
    }

    pub async fn run(&self) -> anyhow::Result<()>{
        println!("Server listening on {}", self.address);
        let address = self.address.clone();
        Server::builder()
            .add_service(ConnectionManagerServer::new(self.clone()))
            .serve(address.parse().unwrap())
            .await.unwrap();
        Ok(())
    }

}


#[tonic::async_trait]
impl ConnectionManager for GrpcServer {
    async fn create_rdma_server(
        &self,
        request: Request<RdmaServerRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let connection_request = request.into_inner();
        let mut client = self.server_manager_client.clone();
        match client.create_rdma_server(connection_request.client_id).await{
            Ok(_) => {},
            Err(e) => {
                return Err(Status::internal(e.to_string()));
            }
        }

        let connection_response = StatusResponse{
            msg: "Success".to_string()
        };
        Ok(Response::new(connection_response))
    }
    async fn create_queue_pair(
        &self,
        request: Request<QueuePairRequest>,
    ) -> Result<Response<QueuePairResponse>, Status> {
        let connection_request = request.into_inner();
        let mut client = self.server_manager_client.clone();
        let qp_gid = client.create_queue_pair(connection_request).await.unwrap();
        Ok(Response::new(qp_gid))
    }

    async fn create_memory_region(
        &self,
        request: Request<MemoryRegionRequest>,
    ) -> Result<Response<MemoryRegionResponse>, Status> {
        let memory_region_request = request.into_inner();
        let mut client = self.server_manager_client.clone();
        let memory_region = client.create_memory_region(memory_region_request).await.unwrap();
        Ok(Response::new(memory_region))
    }
    


}