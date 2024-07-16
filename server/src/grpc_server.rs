use crate::{
    connection_manager::connection_manager::{
    connection_manager_server::{
        ConnectionManager,
        ConnectionManagerServer
    }, ConnectQpRequest, ListenRequest, ListenResponse, QpRequest, QpResponse, RdmaServerRequest, StatusResponse
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

    async fn create_qp(
        &self,
        request: Request<QpRequest>,
    ) -> Result<Response<QpResponse>, Status> {
        let connection_request = request.into_inner();
        let mut client = self.server_manager_client.clone();
        let rdma_port = client.create_qp(connection_request.client_id).await.unwrap();
        let connection_response = QpResponse{
            port: rdma_port as u32
        };
        Ok(Response::new(connection_response))
    }

    async fn request_qp_connection(
        &self,
        request: Request<ConnectQpRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let connection_request = request.into_inner();
        let mut client = self.server_manager_client.clone();
        let _rdma_port = client.connect_qp(connection_request.client_id, connection_request.port as u16).await;
        let connection_response = StatusResponse::default();
        Ok(Response::new(connection_response))
    }

    async fn qp_listen(
        &self,
        request: Request<ListenRequest>,
    ) -> Result<Response<ListenResponse>, Status> {
        let connection_request = request.into_inner();
        let mut client = self.server_manager_client.clone();
        let _rdma_port = client.listen(connection_request.client_id, connection_request.port as u16).await;
        let connection_response = ListenResponse::default();
        Ok(Response::new(connection_response))
    }

}