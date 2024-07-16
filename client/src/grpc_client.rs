use crate::connection_manager::connection_manager::{
    connection_manager_client::ConnectionManagerClient, ConnectQpRequest, ListenRequest, QpRequest, RdmaServerRequest
};
use tonic::Request;

pub struct GrpcClient{
    address: String,
    client_id: u32,
}

impl GrpcClient{
    pub async fn create_rdma_server(&self) -> anyhow::Result<()>{
        let address = self.address.clone();
        let client_id = self.client_id;
        let mut client = ConnectionManagerClient::connect(address).await.unwrap();
        let request = Request::new(RdmaServerRequest{client_id});
        let _response = client.create_rdma_server(request).await.unwrap().into_inner();
        Ok(())
    }
    pub async fn listen(&self, port: u16) -> anyhow::Result<()>{
        let address = self.address.clone();
        let client_id = self.client_id;
        let mut client = ConnectionManagerClient::connect(address).await.unwrap();
        let request = Request::new(ListenRequest{client_id, port: port as u32});
        let _response = client.qp_listen(request).await.unwrap().into_inner();
        Ok(())
    }
    pub async fn create_qp(&self) -> anyhow::Result<u16>{
        let address = self.address.clone();
        let client_id = self.client_id;
        let mut client = ConnectionManagerClient::connect(address).await.unwrap();
        let request = Request::new(QpRequest{client_id});
        let response = client.create_qp(request).await.unwrap().into_inner();
        Ok(response.port as u16)
    }
    pub async fn request_qp_connection(&self, port: u16) -> anyhow::Result<()>{
        let address = self.address.clone();
        let client_id = self.client_id;
        let mut client = ConnectionManagerClient::connect(address).await.unwrap();
        let request = Request::new(ConnectQpRequest{client_id, port: port as u32});
        let _response = client.request_qp_connection(request).await.unwrap().into_inner();
        Ok(())
    }
    pub fn new(address: String, client_id: u32) -> Self{
        GrpcClient{
            address,
            client_id,
        }
    }
}