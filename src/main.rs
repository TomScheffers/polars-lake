#![allow(dead_code)]

use polars::prelude::*;
use polars::toggle_string_cache;
use polars::frame::DataFrame;
use arrow2::io::ipc::read;
use anyhow::Result;

use std::{
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
    sync::{RwLock, Mutex},
    collections::HashMap,
};

mod dataset;
mod storage;
mod buckets;

use dataset::Dataset;

fn main() -> Result<()> {
    toggle_string_cache(true);

    // Mutex testing
    let m = RwLock::new(0);
    let a = m.read().unwrap();
    println!("{}", a);

    // Load stock_current dataset
    let sc = Dataset::from_storage(&"data/stock_parts".to_string())?; // ("data/stock_current/org_key=1/file.parquet", args).unwrap().collect().unwrap(); 

    // Create a mapping of dataset to share between threads
    let mut datasets_map: HashMap<String, Mutex<Dataset>> = HashMap::new();
    datasets_map.insert("stock_current".to_string(), Mutex::new(sc));
    let datasets = Arc::new(datasets_map);

    // Listen on port for ipc streams of chunks
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        stream_into_dataset(stream, &datasets)?;
    }
    Ok(())
}

fn stream_into_dataset(stream: TcpStream, datasets: &Arc<HashMap<String, Mutex<Dataset>>>) -> Result<()> {
    // let metadata = read::read_stream_metadata(&mut stream)?;
    let df = stream_to_df(stream)?.collect()?;
    println!("{:?}", df);
    let keys = vec!["store_key".to_string(), "sku_key".to_string()];
    let mut guard = datasets.as_ref().get("stock_current").unwrap().lock().unwrap();
    let ds = (*guard).upsert(df, keys, false)?;
    *guard = ds;

    Ok(())
}

fn stream_to_df(mut stream: TcpStream) -> Result<LazyFrame> {
    let metadata = read::read_stream_metadata(&mut stream)?;
    println!("{:?}", metadata.schema.metadata);
    let fields = (&metadata).schema.fields.clone();
    let mut reader = read::StreamReader::new(stream, metadata, None);

    let mut dfs = Vec::new(); 
    loop {
        match reader.next() {
            Some(x) => match x? {
                read::StreamState::Some(b) => {
                    let df = DataFrame::try_from((b, fields.as_slice()))?;
                    dfs.push(df.lazy());
                }
                read::StreamState::Waiting => thread::sleep(Duration::from_millis(100)),
            },
            None => break,
        };
    }
    let df = concat(dfs, true, true)?;
    Ok(df)
}