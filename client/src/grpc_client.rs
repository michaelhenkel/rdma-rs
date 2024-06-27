use crate::connection_manager::connection_manager::{
    connection_manager_client::ConnectionManagerClient, ConnectRequest
};
use tonic::Request;

pub struct GrpcClient{
    address: String,
    client_id: u32,
}

impl GrpcClient{
    pub async fn request_connection(&self) -> anyhow::Result<u32>{
        let address = self.address.clone();
        let client_id = self.client_id;
        let mut client = ConnectionManagerClient::connect(address).await.unwrap();
        let request = Request::new(ConnectRequest{client_id});
        let response = client.request_connection(request).await.unwrap().into_inner();
        Ok(response.server_port)
    }
    pub async fn listen(&self) -> anyhow::Result<()>{
        let address = self.address.clone();
        let client_id = self.client_id;
        let mut client = ConnectionManagerClient::connect(address).await.unwrap();
        let request = Request::new(ConnectRequest{client_id});
        let _response = client.listen(request).await.unwrap().into_inner();
        Ok(())
    }
    pub fn new(address: String, client_id: u32) -> Self{
        GrpcClient{
            address,
            client_id,
        }
    }
}