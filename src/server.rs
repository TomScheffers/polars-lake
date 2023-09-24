use tonic::{transport::Server, Request, Response, Status};
use anyhow::Result;
use polars::enable_string_cache;
use polars_io::ipc::{IpcCompression, IpcWriter, IpcReader};
use polars_io::{SerReader, SerWriter};

mod dataset;
mod storage;
mod buckets;
mod database;

use dataset::Dataset;
use database::{TableName, Database};

pub mod db {
    tonic::include_proto!("db");
}

use db::db_server::{Db, DbServer};
use db::{Sql, SourceIpc, ResultIpc, Message, Table};

pub struct MyDbServer {
    database: Database
}

impl MyDbServer {
    pub fn new(database: Database) -> Self {
        Self { database }
    }
}

#[tonic::async_trait]
impl Db for MyDbServer {
    async fn create_table(
        &self,
        request: Request<SourceIpc>,
    ) -> Result<Response<Message>, Status> {
        let r = request.into_inner();
        println!("Got request for {}, {}, {}", r.schema, r.table, r.data.len());
        match IpcReader::new(std::io::Cursor::new(r.data)).finish() {
            Ok(df) => {
                let parts: Option<Vec<String>> = match r.partitions {
                    Some(p) => Some(p.split("/").map(String::from).collect()),
                    None => None,
                };
                let buckets = match r.buckets {
                    Some(p) => Some(p.split("/").map(String::from).collect()),
                    None => None,    
                };
                match Dataset::from_dataframe(df, parts, buckets, None) {
                    Ok(ds) => {
                        self.database.register(r.schema, r.table, ds);
                        Ok(Response::new(Message{ message: "Created table".to_string()}))
                    },
                    Err(e) => Err(Status::internal(e.to_string()))
                }
            },
            Err(e) => Err(Status::internal(e.to_string()))
        }
    }

    async fn insert_table(
        &self,
        request: Request<SourceIpc>,
    ) -> Result<Response<Message>, Status> {
        let r = request.into_inner();
        println!("Got request for {}, {}, {}", r.schema, r.table, r.data.len());
        match IpcReader::new(std::io::Cursor::new(r.data)).finish() {
            Ok(df) => {
                let t = self.database.tables.read().unwrap();
                let ds = t.get(&TableName{schema: r.schema, name: r.table}).unwrap();
                match ds.insert(df, false) {
                    Ok(_) => Ok(Response::new(Message{ message: "Upsert succeeded".to_string()})),
                    Err(e) => Err(Status::internal(e.to_string()))
                }            },
            Err(e) => Err(Status::internal(e.to_string()))
        }
    }

    async fn upsert_table(
        &self,
        request: Request<SourceIpc>,
    ) -> Result<Response<Message>, Status> {
        let r = request.into_inner();
        println!("Got request for {}, {}, {}", r.schema, r.table, r.data.len());
        match IpcReader::new(std::io::Cursor::new(r.data)).finish() {
            Ok(df) => {
                let t = self.database.tables.read().unwrap();
                let ds = t.get(&TableName{schema: r.schema, name: r.table}).unwrap();
                let keys = vec!["store_key".to_string(), "sku_key".to_string()]; 
                match ds.upsert(df, keys, false) {
                    Ok(_) => Ok(Response::new(Message{ message: "Upsert succeeded".to_string()})),
                    Err(e) => Err(Status::internal(e.to_string()))
                }
            },
            Err(e) => Err(Status::internal(e.to_string()))
        }
    }

    async fn materialize_table(
        &self,
        request: Request<Table>,
    ) -> Result<Response<Message>, Status> {
        let r = request.into_inner();
        println!("Got request for {}, {}", r.schema, r.table);
        let t = self.database.tables.read().unwrap();
        let ds = t.get(&TableName{schema: r.schema, name: r.table}).unwrap();
        match ds.collect() {
            Ok(_) => Ok(Response::new(Message{ message: "Materialize succeeded".to_string()})),
            Err(e) => Err(Status::internal(e.to_string()))
        }
    }

    async fn select_ipc(
        &self,
        request: Request<Sql>,
    ) -> Result<Response<ResultIpc>, Status> {
        match self.database.execute_sql(request.into_inner().sql) {
            Ok(mut df) => {
                let mut buf = Vec::new();
                IpcWriter::new(&mut buf).with_compression(Some(IpcCompression::ZSTD)).finish(&mut df).expect("Writing to buf failed");
                println!("Buffer length {}", buf.len());
                Ok(Response::new(ResultIpc{data: buf}))
            },
            Err(e) => Err(Status::internal(e.to_string()))
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    enable_string_cache(true);
    let sv = MyDbServer::new(Database::new());
    let addr = "[::1]:50051".parse()?;
    Server::builder()
        .add_service(DbServer::new(sv))
        .serve(addr)
        .await?;

    Ok(())
}