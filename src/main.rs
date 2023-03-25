#![allow(dead_code)]

use polars::prelude::*;
use polars::toggle_string_cache;
use polars::frame::DataFrame;
use arrow2::io::ipc::read;
use anyhow::Result;

use std::{
    net::{TcpListener, TcpStream},
    thread,
    time::{SystemTime, Duration},
    sync::RwLock,
    collections::HashMap,
};

mod dataset;
mod storage;
mod buckets;

use dataset::Dataset;

// TODO:

// Server:
// Switch to Rocket as a server (with smart routes)

// Upsert:
// Existing dataset: 1. Split upsert df into dataset, 2. align dataset parts, 3.1. existing piece: get lock on dataset part + upsert (+ save if needed), 3.2 new piece: get lock on dataset to append new part

// General:
// Add delete operation (anti right)
// Add drop duplicates (when creating part within dataset if keys present)
// Add schema evolution (upsert contains more / less columns than current dataset)
// Add CREATED_AT & CHANGED_AT columns + update in upsert
// Add S3 storage locations

fn main() -> Result<()> {
    toggle_string_cache(true);

    // Load stock_current dataset
    let sc = Dataset::from_storage(&"data/stock_parts".to_string())?; // ("data/stock_current/org_key=1/file.parquet", args).unwrap().collect().unwrap(); 

    // Create a mapping of dataset to share between threads
    let mut datasets_map: HashMap<String, RwLock<Dataset>> = HashMap::new();
    datasets_map.insert("stock_current".to_string(), RwLock::new(sc));
    let datasets = Arc::new(datasets_map);

    // Listen on port for ipc streams of chunks
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let ldatasets = datasets.clone();
        thread::spawn(|| {
            let r = stream_into_dataset(stream, ldatasets);
            match r {
                Ok(_) => println!("Upsert succes"),
                Err(_) => println!("Error in upsert")
            }
        });
    }
    Ok(())
}

fn stream_into_dataset(stream: TcpStream, datasets: Arc<HashMap<String, RwLock<Dataset>>>) -> Result<()> {
    let start = SystemTime::now();
    // let metadata = read::read_stream_metadata(&mut stream)?;
    let table = "stock_current";
    let df = stream_to_df(stream)?.collect()?;
    let keys = vec!["store_key".to_string(), "sku_key".to_string()];
    let ds = datasets.get(table).unwrap().read().unwrap();
    ds.upsert(df, keys, true)?;
    println!("Upsert table took: {} ms.", start.elapsed().unwrap().as_millis());
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