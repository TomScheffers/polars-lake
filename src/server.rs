use std::collections::HashMap;
use std::sync::mpsc::{channel, Sender, Receiver};
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
    database: Arc<Database>
}

impl MyDbServer {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }
}

// impl MyDbServer {
//     // async fn read_source(&self, r: SourceIpc, count: usize) -> Result<DataFrame> {
//     //     println!("STARTED {}", count);
//     //     let df = IpcReader::new(std::io::Cursor::new(r.data)).finish()?;
//     //     println!("FINISHED {}", count);
//     //     Ok(df)
//     // }

//     // async fn insert_source(&self, r: SourceIpc) -> Result<()> {
//     //     let df = IpcReader::new(std::io::Cursor::new(r.data)).finish()?;
//     //     let t = self.database.tables.read().unwrap();
//     //     let ds = t.get(&TableName{schema: r.schema, name: r.table}).unwrap();
//     //     ds.insert(df, false)?;
//     //     Ok(())
//     // }
// }

async fn read_source(r: SourceIpc, tx: Sender<DataFrame>) {
    // println!("STARTED {}", count);
    let df = IpcReader::new(std::io::Cursor::new(r.data)).finish();
    match df {
        Ok(df) => {
            let _f = tx.send(df);
        },
        Err(e) => {
            println!("{}", e);
        }
    }
    // println!("FINISHED {}", count);
}

async fn consume_sources(database: Arc<Database>, schema: String, table: String, rx: Receiver<DataFrame>) {
    let mut dfs = Vec::new();
    let mut rows = 0;

    while let Ok(df) = rx.recv() {
        rows += df.height();
        dfs.push(df);
        if rows > 1_000_000 {
            let mut dfm = Vec::new();
            while dfs.len() > 0 {
                dfm.push(dfs.pop().unwrap())
            }
            // Find schema, table & concat frames
            let df = concat(dfm.into_iter().map(|df| df.lazy()).collect::<Vec<LazyFrame>>(), UnionArgs::default()).unwrap().collect().unwrap();
            println!("Writing data of size {:?}", df.shape());

            // Insert into dataset
            let tables = database.tables.lock().unwrap();
            let ds = tables.get(&TableName{schema: schema.clone(), name: table.clone()}).unwrap();
            ds.insert(df, false).unwrap();

            rows = 0;
        }
    }

    // Find schema, table & concat frames
    let df = concat(dfs.into_iter().map(|df| df.lazy()).collect::<Vec<LazyFrame>>(), UnionArgs::default()).unwrap().collect().unwrap();
    println!("Writing data of size {:?}", df.shape());

    // Insert into dataset
    let tables = database.tables.lock().unwrap();
    let ds = tables.get(&TableName{schema: schema.clone(), name: table.clone()}).unwrap();
    ds.insert(df, false).unwrap();
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

        let df = IpcReader::new(std::io::Cursor::new(r.data)).finish().unwrap();
        let t = self.database.tables.lock().unwrap();
        let ds = t.get(&TableName{schema: r.schema, name: r.table}).unwrap();
        match ds.insert(df, false) {
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
                let t = self.database.tables.lock().unwrap();
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
        let mut tasks = Vec::new();

        // First message
        let r = stream.message().await.unwrap().unwrap();
        println!("FIRST MESSAGE");
        let schema = r.schema.clone();
        let table = r.table.clone();
        let partitions = if r.partitions.len() > 0 {Some(r.partitions.clone())} else {None};
        let buckets = if r.buckets.len() > 0 {Some(r.buckets.clone())} else {None};

        // Create channel
        let (tx, rx) = channel::<DataFrame>();

        // Read Ipc stream
        tasks.push(tokio::spawn(read_source(r, tx.clone())));
        while let Ok(Some(r)) = stream.message().await {
            tasks.push(tokio::spawn(read_source(r, tx.clone())));
        };

        drop(tx);

        let mut dfs = Vec::new();
        while let Ok(df) = rx.recv() {
            dfs.push(df);
        }

        // Find schema, table & concat frames
        let df = concat(dfs.into_iter().map(|df| df.lazy()).collect::<Vec<LazyFrame>>(), UnionArgs::default()).unwrap().collect().unwrap();
        println!("Got data of size {:?}", df.shape());

        match Dataset::from_dataframe(df, partitions, buckets, None) {
            Ok(ds) => {
                self.database.register(schema, table, ds);
                Ok(Response::new(Message{ message: "Created table".to_string()}))
            },
            Err(e) => Err(Status::internal(e.to_string()))
        }
    }

    async fn insert_table_stream(
        &self,
        request: Request<tonic::Streaming<SourceIpc>>,
    ) -> Result<Response<Message>, Status> {
        let mut stream = request.into_inner();
        let mut tasks = Vec::new();

        // First message
        let r = stream.message().await.unwrap().unwrap();
        println!("FIRST MESSAGE");
        let schema = r.schema.clone();
        let table = r.table.clone();

        // Create channel
        let (tx, rx) = channel::<DataFrame>();

        // Spawn consumer
        tasks.push(tokio::spawn(consume_sources(self.database.clone(), schema, table, rx)));

        // Read ipcs
        tasks.push(tokio::spawn(read_source(r, tx.clone())));
        while let Ok(Some(r)) = stream.message().await {
            tasks.push(tokio::spawn(read_source(r, tx.clone())));
        };

        drop(tx);

        Ok(Response::new(Message{ message: "Async insert succeeded".to_string()}))
    }



    async fn materialize_table(
        &self,
        request: Request<Table>,
    ) -> Result<Response<Message>, Status> {
        let r = request.into_inner();
        let t = self.database.tables.lock().unwrap();
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
    let sv = MyDbServer::new(Arc::new(Database::new()));
    let addr = "[::1]:50051".parse()?;
    Server::builder()
        .concurrency_limit_per_connection(100)
        .add_service(DbServer::new(sv))
        .serve(addr)
        .await?;

    Ok(())
}