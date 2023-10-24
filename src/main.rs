#![allow(dead_code)]

use polars::prelude::*;
use polars::enable_string_cache;
use polars_io::ipc::IpcStreamReader;

use anyhow::Result;

use std::{
    net::{TcpListener, TcpStream},
    thread,
    time::SystemTime,
    collections::HashMap,
};

mod dataset;
mod storage;
mod buckets;
mod database;

use dataset::Dataset;
use database::Database;

// Database:
// Save database in manifest file

// Datasets:
// Browse datasets from root

// General:
// Add delete operation (anti right)
// Add drop duplicates (when creating part within dataset if keys present)
// Add schema evolution (upsert contains more / less columns than current dataset)
// Add CREATED_AT & CHANGED_AT columns + update in upsert
// Add S3 storage locations (async offloading)

fn main() -> Result<()> {
    enable_string_cache(true);

    // Load stock_current dataset
    let sc = Dataset::from_storage(&"data/stock_parts".to_string(), false)?;

    // Create a mapping of dataset to share between threads
    let mut datasets_map: HashMap<String, Dataset> = HashMap::new();
    datasets_map.insert("stock_current".to_string(), sc);
    let datasets = Arc::new(datasets_map);

    // Database
    let sc = Dataset::from_storage(&"data/stock_parts".to_string(), false)?;
    let db = Database::new();
    db.register("public".to_string(), "stock_current".to_string(), sc);

    let start = SystemTime::now();
    let df = db.execute_sql("SELECT * FROM stock_current WHERE store_key = 101;".to_string())?;
    println!("SQL took: {} ms.", start.elapsed().unwrap().as_millis());
    println!("{:?}", df);


    // Listen on port for ipc streams of chunks
    let listener = TcpListener::bind("127.0.0.1:7879").unwrap();
    println!("Listening on port");
    for stream in listener.incoming() {
        let stream = stream.unwrap();
        let ldatasets = datasets.clone();
        thread::spawn(|| {
            match stream_into_dataset(stream, ldatasets) {
                Ok(_) => {},
                Err(e) => println!("Error in upsert {:?}", e)
            }
        });
    }
    Ok(())
}

fn stream_into_dataset(stream: TcpStream, datasets: Arc<HashMap<String, Dataset>>) -> Result<()> {
    let start = SystemTime::now();
    let df = IpcStreamReader::new(stream).finish()?;
    let ds = datasets.get("stock_current").unwrap();
    let keys = vec!["store_key".to_string(), "sku_key".to_string()];
    ds.upsert(df, keys.clone(), false)?;
    println!("Upsert table took: {} ms.", start.elapsed().unwrap().as_millis());
    Ok(())
}
