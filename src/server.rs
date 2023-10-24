use std::sync::mpsc::{channel, Sender, Receiver};
use tokio_stream::Stream;
use std::pin::Pin;
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
use db::{Sql, SourceIpc, SqlResults, Message, Table, TableInfo};

pub struct MyDbServer {
    database: Arc<Database>
}

impl MyDbServer {
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }
}

async fn read_source(r: SourceIpc, tx: Sender<DataFrame>) {
    let df = IpcReader::new(std::io::Cursor::new(r.data)).finish();
    match df {
        Ok(df) => {
            let _f = tx.send(df);
        },
        Err(e) => {
            println!("{}", e);
        }
    }
}

async fn consume_sources(database: Arc<Database>, schema: String, table: String, keys: Vec<String>, rx: Receiver<DataFrame>) {
    let mut dfs = Vec::new();
    let mut rows = 0;

    while let Ok(df) = rx.recv() {
        rows += df.height();
        dfs.push(df);
        if rows > 10_000_000 {
            let mut dfm = Vec::new();
            while dfs.len() > 0 {
                dfm.push(dfs.pop().unwrap())
            }
            // Find schema, table & concat frames
            let df = concat(dfm.into_iter().map(|df| df.lazy()).collect::<Vec<LazyFrame>>(), UnionArgs::default()).unwrap().collect().unwrap();
            println!("Writing data of size {:?}", df.shape());

            if keys.len() > 0 {
                // Upsert into dataset
                let tables = database.tables.lock().unwrap();
                let ds = tables.get(&TableName{schema: schema.clone(), name: table.clone()}).unwrap();
                ds.upsert(df, keys.clone(), false).unwrap();
            } else {
                // Insert into dataset
                let tables = database.tables.lock().unwrap();
                let ds = tables.get(&TableName{schema: schema.clone(), name: table.clone()}).unwrap();
                ds.insert(df, false).unwrap();
            }

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

    async fn insert_table(
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
        tasks.push(tokio::spawn(consume_sources(self.database.clone(), schema, table, vec![], rx)));

        // Read ipcs
        tasks.push(tokio::spawn(read_source(r, tx.clone())));
        while let Ok(Some(r)) = stream.message().await {
            tasks.push(tokio::spawn(read_source(r, tx.clone())));
        };
        drop(tx);
        Ok(Response::new(Message{ message: "Async insert succeeded".to_string()}))
    }

    async fn upsert_table(
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
        let keys = r.keys.clone();

        // Create channel
        let (tx, rx) = channel::<DataFrame>();

        // Spawn consumer
        tasks.push(tokio::spawn(consume_sources(self.database.clone(), schema, table, keys, rx)));

        // Read ipcs
        tasks.push(tokio::spawn(read_source(r, tx.clone())));
        while let Ok(Some(r)) = stream.message().await {
            tasks.push(tokio::spawn(read_source(r, tx.clone())));
        };
        drop(tx);
        Ok(Response::new(Message{ message: "Async upsert succeeded".to_string()}))
    }

    async fn materialize_table(
        &self,
        request: Request<Table>,
    ) -> Result<Response<Message>, Status> {
        let r = request.into_inner();
        let tn = TableName{schema: r.schema, name: r.table};    
        let tables = self.database.tables.lock().unwrap();    
        match tables.get(&tn) {
            Some(ds) => {
                match ds.collect() {
                    Ok(_) => Ok(Response::new(Message{ message: "Materialize succeeded".to_string()})),
                    Err(e) => Err(Status::internal(e.to_string()))
                }
            },
            None => Err(Status::internal("Table does not exist".to_string()))
        }
    }

    async fn get_table_info(
        &self,
        request: Request<Table>,
    ) -> Result<Response<TableInfo>, Status> {
        let r = request.into_inner();
        let tn = TableName{schema: r.schema, name: r.table};
        let tables = self.database.tables.lock().unwrap();
        match tables.get(&tn) {
            Some(ds) => {
                let rows = ds.rows() as u32;
                let parts = ds.parts.lock().unwrap().keys().len() as u32;
                match ds.schema() {
                    Ok(schema) => {
                        let columns = schema.iter_names().map(|n| n.as_str().to_string()).collect();
                        let dtypes = schema.iter_dtypes().map(|dt| dt.to_string()).collect();
                        Ok(Response::new(TableInfo{ columns, dtypes, rows, parts}))
                    },
                    Err(e) => Err(Status::internal(e.to_string()))
                }
            },
            None => Err(Status::internal("Table does not exist".to_string()))
        }
    }

    // async fn select_ipc(
    //     &self,
    //     request: Request<tonic::Streaming<Sql>>,
    // ) -> Result<Response<ReceiverStream<Result<SqlResults, Status>>>, Status> {
    //     let mut stream = request.into_inner();

    //     // Channel for streaming results
    //     let (tx, rx) = channel();

    //     // Read ipcs
    //     let mut sql_map = HashMap::new();
    //     let cnt = 0;
    //     while let Ok(Some(r)) = stream.message().await {
    //         let qid = r.qid.unwrap_or(cnt);
    //         cnt += 1;
    //         sql_map.insert(r.qid, r.sql);

    //         if sql_map.len() > 10 {
    //             let sql_mapd = sql_map.drain();
    //             let qids = sql_map.iter().map(|(k,_)| *k).collect();
    //             let sqls = sql_map.iter().map(|(_,v)| *v).collect();
    //             let dfs = self.database.execute_sqls(&sqls);
    //             let results = dfs.into_iter().map(|(k, mut df)| {
    //                 let rows = df.height() as u32;
    //                 let schema = df.schema();
    //                 let mut buf = Vec::new();
    //                 IpcWriter::new(&mut buf).with_compression(Some(IpcCompression::ZSTD)).finish(&mut df).expect("Writing to buf failed");
    //                 (k, (buf, rows, schema))
    //             }).collect::<HashMap<String, (Vec<u8>, u32, Schema)>>();

    //             for (qid, sql) in qids.iter().zip(sqls) {
    //                 let (buf, rows, schema) = results.get(sql).unwrap();
    //                 let columns = schema.iter_names().map(|n| n.as_str().to_string()).collect();
    //                 let dtypes = schema.iter_dtypes().map(|dt| dt.to_string()).collect();

    //                 tx.send(Ok(SqlResults{ data: buf.clone(), rows: *rows, columns: columns, dtypes: dtypes, qid: qid }));
    //             } 
    //         }
    //     };
    //     Ok(Response::new(ReceiverStream::new(rx)))
    // }

    type SelectIpcStream = Pin<Box<dyn Stream<Item = Result<SqlResults, Status>> + Send  + 'static>>;

    async fn select_ipc(
        &self,
        request: Request<tonic::Streaming<Sql>>,
    ) -> Result<Response<Self::SelectIpcStream>, Status> {
        let mut stream = request.into_inner();
        let db = self.database.clone();
    
        let output = async_stream::try_stream! {
            while let Ok(Some(r)) = stream.message().await {
                match db.execute_sql(r.sql) {
                    Ok(mut df) => {
                        let rows = df.height() as u32;
                        let schema = df.schema();
                        let columns = schema.iter_names().map(|n| n.as_str().to_string()).collect();
                        let dtypes = schema.iter_dtypes().map(|dt| dt.to_string()).collect();

                        let mut buf = Vec::new();
                        IpcWriter::new(&mut buf).with_compression(Some(IpcCompression::ZSTD)).finish(&mut df).expect("Writing to buf failed");

                        yield SqlResults{ data: buf.clone(), rows: rows.clone(), columns: columns, dtypes: dtypes, qid: r.qid }
                    },
                    Err(_e) => continue
                }
            }
        };
        Ok(Response::new(Box::pin(output)))
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