#![allow(dead_code)]

use polars::prelude::*;
use polars::toggle_string_cache;
use std::time::SystemTime;
use anyhow::Result;

mod dataset;
mod storage;
mod buckets;

use crate::dataset::Dataset;
use crate::storage::*;

// TODO:
// Add delete operation (anti right)
// Add drop duplicates (when creating part within dataset if keys present)
// Add append operation (new parts?)
// Add schema evolution (upsert contains more / less columns than current dataset)
// Add CREATED_AT & CHANGED_AT columns + update in upsert
// Add S3 storage locations
// Add rebucketing operation
// Add way to have multiple parts per bucket / partition

fn main() ->  Result<()> {
    toggle_string_cache(true);

    let start = SystemTime::now();
    let args = ScanArgsParquet::default();
    let df = LazyFrame::scan_parquet("data/stock_current/org_key=1/file.parquet", args)?.collect()?; 
    println!("Reading table took: {} ms. Rows {:?}", start.elapsed().unwrap().as_millis(), df.shape());

    // Dataset from DataFrame
    let start = SystemTime::now();
    let parts = Vec::new(); //vec!["org_key".to_string()];
    let buckets = vec!["sku_key".to_string()];
    let ds = Dataset::from_dataframe(df, Some(parts), Some(buckets), None)?;
    println!("Creating dataset from dataframe took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let store = DatasetStorage::new("data/stock_parts".to_string(), Format::Parquet, Some(Compression::Snappy));
    let ds = ds.with_storage(Some(store));
    ds.to_storage();
    println!("Saving dataset took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let ds = Dataset::from_storage(&"data/stock_parts".to_string())?; // ("data/stock_current/org_key=1/file.parquet", args).unwrap().collect().unwrap(); 
    println!("Reading dataset took: {} ms.", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let args = ScanArgsParquet::default();
    let df = LazyFrame::scan_parquet("data/stock_current/org_key=1/file.parquet", args)?.collect()?; 
    println!("Reading table took: {} ms. Rows {:?}", start.elapsed().unwrap().as_millis(), df.shape());

    let start = SystemTime::now();
    let dfu = df.head(Some(10000));
    let keys = vec!["store_key".to_string(), "sku_key".to_string()];
    let _ = ds.upsert(dfu, keys)?;
    println!("Upsert table took: {} ms.", start.elapsed().unwrap().as_millis());

    Ok(())

    // Utf8 to Struct

    // let props = df.column("properties").unwrap().utf8();
    // let iter = props.into_iter().flat_map(|x| x.into_iter()).map(|v| v.unwrap_or("null"));
    // let dtype = ndjson::read::infer_iter(iter.take(10)).unwrap();
    // println!("{:?}", dtype);

    // let start = SystemTime::now();
    // let props = df.column("properties").unwrap().utf8();
    // let iter = props.into_iter().flat_map(|x| x.into_iter()).map(|v| v.unwrap_or("null"));
    // let _array = ndjson::read::deserialize_iter(iter.take(1000), dtype).unwrap();
    // println!("Reading JSON column took: {} ms", start.elapsed().unwrap().as_millis());
}
