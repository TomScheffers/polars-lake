#![allow(dead_code)]

use polars::prelude::*;
use std::time::SystemTime;

mod dataset;
mod storage;
mod buckets;

use crate::dataset::Dataset;
use crate::storage::*;
// use std::path::PathBuf;
// use arrow2::io::ndjson;

fn main() {
    let start = SystemTime::now();
    let _ = Dataset::from_storage(&"data/stock_parts".to_string()); // ("data/stock_current/org_key=1/file.parquet", args).unwrap().collect().unwrap(); 
    println!("Reading dataset took: {} ms.", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let args = ScanArgsParquet::default();
    let df = LazyFrame::scan_parquet("data/stock_current/org_key=1/file.parquet", args).unwrap().collect().unwrap(); 
    println!("Reading table took: {} ms. Rows {:?}", start.elapsed().unwrap().as_millis(), df.shape());

    // Cast to categorical
    // df.replace("option_name", df.column("option_name").unwrap().cast(&DataType::Utf8).unwrap().cast(&DataType::Categorical(None)).unwrap()).unwrap();
    
    // Presto bucketing 000234_0_20180102_030405_00641_x1y2z

    // Implement anyhow for results of functions https://docs.rs/anyhow/latest/anyhow/

    // Dataset from DataFrame
    let start = SystemTime::now();
    let parts = Vec::new(); //vec!["org_key".to_string()];
    let buckets = vec!["sku_key".to_string()];
    let ds = Dataset::from_dataframe(df, Some(parts), Some(buckets), None);
    println!("Creating dataset from dataframe took: {} ms", start.elapsed().unwrap().as_millis());

    let start = SystemTime::now();
    let store = DatasetStorage::new("data/stock_parts".to_string(), Format::Parquet, Some(Compression::Snappy));
    let ds = ds.with_storage(Some(store));
    ds.to_storage();
    println!("Saving dataset took: {} ms", start.elapsed().unwrap().as_millis());

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


    // File sinking

    // let args = ScanArgsParquet::default();
    // let df = LazyFrame::scan_parquet("data/skus/org_key=1/file.parquet", args).unwrap();   
    // println!("{:#?}", df.schema().unwrap()); 
    // let path = PathBuf::from(r"data/agg.parquet");
    // let wo = ParquetWriteOptions::default();
    // let _dfp = df.groupby(["group_key", "collection_key"])
    //     .agg([
    //         col("sku_key").min().alias("min"),
    //         col("sku_key").max().alias("max"),
    //         col("sku_key").count().alias("cnt"),
    //     ])
    //     .sink_parquet(path, wo)
    //     .unwrap();

    // let start = SystemTime::now();
    // let args = ScanArgsParquet::default();
    // let df = LazyFrame::scan_parquet("data/agg.parquet", args).unwrap().collect().unwrap();  
    // println!("Reading table took: {} ms", start.elapsed().unwrap().as_millis());  
    // println!("{:?}", &df);
}