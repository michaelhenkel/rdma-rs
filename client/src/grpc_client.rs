use crate::connection_manager::connection_manager::{
    connection_manager_client::ConnectionManagerClient, MemoryRegionRequest, MemoryRegionResponse, QueuePairRequest, QueuePairResponse, RdmaServerRequest
};
use common::{Family, Mode};
use tonic::Request;

pub struct GrpcClient{
    address: String,
    client_id: u32,
    mode: Mode,
    family: Family,
    qpns: u32,
}

impl GrpcClient{
    pub async fn create_rdma_server(&self) -> anyhow::Result<()>{
        let address = self.address.clone();
        let client_id = self.client_id;
        let mode = self.mode.clone().into();
        let family = self.family.clone().into();
        let qpns = self.qpns;
        let mut client = ConnectionManagerClient::connect(address).await.unwrap();
        let request = Request::new(RdmaServerRequest{client_id, mode, family, qpns});
        let _response = client.create_rdma_server(request).await.unwrap().into_inner();
        Ok(())
    }
    pub async fn create_queue_pair(&self, mut qp_request: QueuePairRequest) -> anyhow::Result<QueuePairResponse>{
        let address = self.address.clone();
        let client_id = self.client_id;
        qp_request.client_id = client_id;
        let mut client = ConnectionManagerClient::connect(address).await.unwrap();
        let request = Request::new(qp_request);
        let response_qp_gid = client.create_queue_pair(request).await.unwrap().into_inner();
        Ok(response_qp_gid)
    }
    pub async fn create_memory_region(&self, memory_region_request: MemoryRegionRequest) -> anyhow::Result<MemoryRegionResponse>{
        let address = self.address.clone();
        let mut client = ConnectionManagerClient::connect(address).await.unwrap();
        let request = Request::new(memory_region_request);
        let response = client.create_memory_region(request).await.unwrap().into_inner();
        Ok(response)
    }
    pub fn new(address: String, client_id: u32, mode: Mode, family: Family, qpns: u32) -> Self{
        GrpcClient{
            address,
            client_id,
            mode,
            family,
            qpns,
        }
    }
}