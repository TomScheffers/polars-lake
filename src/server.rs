use std::collections::HashMap;
use tonic::{transport::Server, Request, Response, Status};
use anyhow::Result;
use polars::enable_string_cache;
use polars::prelude::*;
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
use db::{Sql, Sqls, SourceIpc, ResultIpc, ResultsIpc, Message, Table};

pub struct MyDbServer {
    database: Database
}

impl MyDbServer {
    pub fn new(database: Database) -> Self {
        Self { database }
    }
}

impl MyDbServer {
    async fn read_source(&self, r: SourceIpc) -> Result<DataFrame> {
        Ok(IpcReader::new(std::io::Cursor::new(r.data)).finish()?)
    }

    async fn insert_source(&self, r: SourceIpc) -> Result<()> {
        let df = IpcReader::new(std::io::Cursor::new(r.data)).finish()?;
        let t = self.database.tables.read().unwrap();
        let ds = t.get(&TableName{schema: r.schema, name: r.table}).unwrap();
        ds.insert(df, false)?;
        Ok(())
    }
}

#[tonic::async_trait]
impl Db for MyDbServer {
    async fn create_table(
        &self,
        request: Request<SourceIpc>,
    ) -> Result<Response<Message>, Status> {
        let r = request.into_inner();
        match IpcReader::new(std::io::Cursor::new(r.data)).finish() {
            Ok(df) => {
                let parts = if r.partitions.len() > 0 {Some(r.partitions.clone())} else {None};
                let buckets = if r.buckets.len() > 0 {Some(r.buckets.clone())} else {None};
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

        match self.insert_source(r).await {
            Ok(_) => Ok(Response::new(Message{ message: "Upsert succeeded".to_string()})),
            Err(e) => Err(Status::internal(e.to_string()))
        }
    }

    async fn upsert_table(
        &self,
        request: Request<SourceIpc>,
    ) -> Result<Response<Message>, Status> {
        let r = request.into_inner();
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

    async fn create_table_stream(
        &self,
        request: Request<tonic::Streaming<SourceIpc>>,
    ) -> Result<Response<Message>, Status> {
        let mut stream = request.into_inner();
        let mut futures = Vec::new();

        // First message
        let r = stream.message().await.unwrap().unwrap();
        let schema = r.schema.clone();
        let table = r.table.clone();
        let partitions = if r.partitions.len() > 0 {Some(r.partitions.clone())} else {None};
        let buckets = if r.buckets.len() > 0 {Some(r.buckets.clone())} else {None};
        futures.push(self.read_source(r));

        while let Ok(Some(r)) = stream.message().await {
            futures.push(self.read_source(r));
        };
        let dfs = futures::future::join_all(futures).await;

        // Find schema, table & concat frames
        let df = concat(dfs.into_iter().map(|df| df.unwrap().lazy()).collect::<Vec<LazyFrame>>(), UnionArgs::default()).unwrap().collect().unwrap();

        match Dataset::from_dataframe(df, partitions, buckets, None) {
            Ok(ds) => {
                self.database.register(schema, table, ds);
                Ok(Response::new(Message{ message: "Created table".to_string()}))
            },
            Err(e) => Err(Status::internal(e.to_string()))
        }
    }

    // async fn insert_table_stream(
    //     &self,
    //     request: Request<tonic::Streaming<SourceIpc>>,
    // ) -> Result<Response<Message>, Status> {
    //     let mut stream = request.into_inner();
    //     let mut futures = Vec::new();

        // First message
        // let r = stream.message().await.unwrap().unwrap();
        // let schema = r.schema;
        // let table = r.table;

    //     while let Ok(Some(r)) = stream.message().await {
    //         futures.push(self.read_source(r));
    //     };
    //     let dfs = futures::future::join_all(futures).await;

    //     // Find schema, table & concat frames
    //     let df = concat(dfs.into_iter().map(|df| df.unwrap().lazy()).collect::<Vec<LazyFrame>>(), UnionArgs::default()).unwrap().collect().unwrap();

    //     // Insert into dataset
    //     let t = self.database.tables.read().unwrap();
    //     let ds = t.get(&TableName{schema: schema, name: table}).unwrap();
    //     ds.insert(df, false).unwrap();

    //     Ok(Response::new(Message{ message: "Async insert succeeded".to_string()}))
    // }



    async fn materialize_table(
        &self,
        request: Request<Table>,
    ) -> Result<Response<Message>, Status> {
        let r = request.into_inner();
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
                Ok(Response::new(ResultIpc{data: Some(buf)}))
            },
            Err(e) => Err(Status::internal(e.to_string()))
        }
    }

    async fn selects_ipc(
        &self,
        request: Request<Sqls>,
    ) -> Result<Response<ResultsIpc>, Status> {
        let sqls = request.into_inner().sqls.iter().map(|x| x.sql.clone()).collect();
        let buffers = self.database.execute_sqls(&sqls).into_iter().map(|(k, mut df)| {
            let mut buf = Vec::new();
            IpcWriter::new(&mut buf).with_compression(Some(IpcCompression::ZSTD)).finish(&mut df).expect("Writing to buf failed");
            (k, buf)
        }).collect::<HashMap<String, Vec<u8>>>();        
        let results = sqls.into_iter().map(|x| {
            match buffers.get(&x) {
                Some(b) => ResultIpc{data: Some(b.clone())},
                None => ResultIpc{data: None}
            }
        }).collect();
        Ok(Response::new(ResultsIpc{results: results}))
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