use crate::{
    connection_manager::connection_manager::{
    connection_manager_server::{
        ConnectionManager,
        ConnectionManagerServer
    },
    ConnectRequest, ConnectResponse, ListenRequest, ListenResponse
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
    async fn request_connection(
        &self,
        request: Request<ConnectRequest>,
    ) -> Result<Response<ConnectResponse>, Status> {
        let connection_request = request.into_inner();
        let mut client = self.server_manager_client.clone();
        let rdma_port = client.request_connection(connection_request.client_id, connection_request.qps).await;
        let connection_response = ConnectResponse{
            server_port: rdma_port
        };
        Ok(Response::new(connection_response))
    }
    async fn listen(
        &self,
        request: Request<ListenRequest>,
    ) -> Result<Response<ListenResponse>, Status> {
        let connection_request = request.into_inner();
        let mut client = self.server_manager_client.clone();
        let _rdma_port = client.listen(connection_request.client_id, connection_request.qp_idx).await;
        let connection_response = ListenResponse::default();
        Ok(Response::new(connection_response))
    }

}