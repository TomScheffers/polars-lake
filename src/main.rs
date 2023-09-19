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
    collections::HashMap,
};

mod dataset;
mod storage;
mod buckets;

use dataset::Dataset;

// Server:
// Switch to Rocket as a server (with smart routes)

// Database:
// Add schemas with tables (datasets)
// Save database in manifest file

// Datasets:
// Browse datasets from root
// Write new dataset when not present (from stream)
// Read dataset to dataframe (align columns + add missing values)

// General:
// Add delete operation (anti right)
// Add drop duplicates (when creating part within dataset if keys present)
// Add schema evolution (upsert contains more / less columns than current dataset)
// Add CREATED_AT & CHANGED_AT columns + update in upsert
// Add S3 storage locations

fn main() -> Result<()> {
    toggle_string_cache(true);

    // Load stock_current dataset
    let sc = Dataset::from_storage(&"data/stock_parts".to_string())?;

    // Create a mapping of dataset to share between threads
    let mut datasets_map: HashMap<String, Dataset> = HashMap::new();
    datasets_map.insert("stock_current".to_string(), sc);
    let datasets = Arc::new(datasets_map);

    // Listen on port for ipc streams of chunks
    let listener = TcpListener::bind("127.0.0.1:7879").unwrap();
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

fn stream_into_dataset(mut stream: TcpStream, datasets: Arc<HashMap<String, Dataset>>) -> Result<()> {
    let start = SystemTime::now();
    let metadata = read::read_stream_metadata(&mut stream)?;
    let md = (&metadata).schema.metadata.clone();
    let table = md.get("table").unwrap();
    let fields = (&metadata).schema.fields.clone();
    let keys = vec!["store_key".to_string(), "sku_key".to_string()];

    // Read dataframes from stream and concatenate to one dataframe
    let mut reader = read::StreamReader::new(stream, metadata, None);
    loop {
        match reader.next() {
            Some(x) => match x? {
                read::StreamState::Some(b) => {
                    let df = DataFrame::try_from((b, fields.as_slice()))?;
                    let ds = datasets.get(table).unwrap();
                    ds.upsert(df, keys.clone(), false)?;
                    println!("Upsert table took: {} ms.", start.elapsed().unwrap().as_millis());
                }
                read::StreamState::Waiting => thread::sleep(Duration::from_millis(1)),
            },
            None => break
        };
    }
    Ok(())
}
