
use std::{collections::HashMap, sync::{Arc, Mutex}};
use common::*;
use tokio::sync::RwLock;

use crate::qp::Qp;



#[derive(Clone)]
pub struct RdmaServer{
    pub client: RdmaServerClient,
    rx: Arc<RwLock<tokio::sync::mpsc::Receiver<RdmaServerCommand>>>,
    address: String,
}

impl RdmaServer{
    pub fn new(address: String) -> RdmaServer{
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let client = RdmaServerClient::new(tx);
        RdmaServer{
            client,
            rx: Arc::new(RwLock::new(rx)),
            address
        }
    }

    pub async fn run(self) -> anyhow::Result<()>{
        let mut rx = self.rx.write().await;
        let qps: Arc<RwLock<HashMap<u16, Qp>>> = Arc::new(RwLock::new(HashMap::new()));
        let mut pd = None;
        let data: Arc<Mutex<Option<Data>>> = Arc::new(Mutex::new(None));
        while let Some(rdma_server_command) = rx.recv().await{
            let data = data.clone();
            match rdma_server_command{
                RdmaServerCommand::Listen{port, tx} => {
                    let qps = qps.write().await;
                    let qp = qps.get(&port).unwrap();
                    let mut qp = qp.clone();
                    tokio::spawn(async move{
                        loop {
                            let ret = qp.listen(data.clone()).await.unwrap();
                            if ret == 0 {
                                break;
                            }
                        }
                        tx.send(()).unwrap();
                    });
                },
                RdmaServerCommand::ConnectQp { port } => {
                    let qps = qps.clone();
                    tokio::spawn(async move{
                        let mut qps = qps.write().await;
                        let qp = qps.get_mut(&port).unwrap();
                        qp.wait_for_connection().await.unwrap();
                    });
                }
                RdmaServerCommand::CreateQp{tx} => {
                    let address = self.address.clone();
                    let qp = Qp::new(address, &mut pd).unwrap();
                    let port = qp.port;
                    let mut qps = qps.write().await;
                    qps.insert(port, qp);
                    tx.send(port).unwrap();
                }
            }
        }
        println!("rdma server stopped");
        Ok(())
    }
}

#[derive(Clone)]
pub struct RdmaServerClient{
    tx: tokio::sync::mpsc::Sender<RdmaServerCommand>
}

impl RdmaServerClient{
    pub fn new(tx: tokio::sync::mpsc::Sender<RdmaServerCommand>) -> Self{
        RdmaServerClient{
            tx
        }
    }
    pub async fn listen(&mut self, port: u16) -> anyhow::Result<()>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(RdmaServerCommand::Listen{port, tx}).await.unwrap();
        rx.await.unwrap();
        Ok(())
    }
    pub async fn create_qp(&mut self) -> anyhow::Result<u16>{
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx.send(RdmaServerCommand::CreateQp{tx}).await.unwrap();
        Ok(rx.await.unwrap())
    }
    pub async fn connect_qp(&mut self, port: u16) -> anyhow::Result<()>{
        self.tx.send(RdmaServerCommand::ConnectQp{port}).await.unwrap();
        Ok(())
    }

}

pub enum RdmaServerCommand{
    Listen{
        port: u16,
        tx: tokio::sync::oneshot::Sender<()>
    },
    CreateQp{
        tx: tokio::sync::oneshot::Sender<u16>
    },
    ConnectQp{
        port: u16,
    }
}