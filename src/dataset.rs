use polars::prelude::*;
use rayon::prelude::*;
use std::path::PathBuf;
use std::fs;
use std::collections::HashMap;
use std::time::SystemTime;

use serde_json;
use serde::{Serialize, Deserialize};

use crate::storage::DatasetStorage;

pub struct DatasetPart {
    table: Option<DataFrame>, // For lazy loading
    filters: Option<HashMap<String, String>>,
    path: Option<String>,
}

impl DatasetPart {
    pub fn new( table: Option<DataFrame>, filters: Option<HashMap<String, String>>, path: Option<String>) -> Self {
        Self { table, filters, path }
    }

    pub fn load(self) -> Self {
        self
    }

    pub fn partition_path(&self, partitions: &Option<Vec<String>>) -> String {
        match &partitions {
            Some(partitions) => {
                let mut parts = Vec::new();
                for p in partitions {
                    let v = self.filters.as_ref().unwrap().get(p).unwrap();
                    parts.push(format!("{}={}", p, v));
                }
                parts.join("/")
            },
            None => "".to_string()
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Dataset {
    pub partitions: Option<Vec<String>>, // File based partitioning columns
    pub buckets: Option<Vec<String>>, // Hash bucketing columns (within partitions)
    #[serde(skip_serializing, skip_deserializing)]
    pub parts: Vec<DatasetPart>, // Underlying parts (referencing to tables)
    pub storage: Option<DatasetStorage> // Storage options
}

impl Dataset {
    pub fn new(partitions: Option<Vec<String>>, buckets: Option<Vec<String>>, parts: Vec<DatasetPart>, storage: Option<DatasetStorage>) -> Self {
        Self { partitions, buckets, parts, storage }
    }

    pub fn from_dataframe(df: DataFrame, partitions: &Vec<String>, storage: Option<DatasetStorage>) -> Self {
        let dfp = df.partition_by(partitions).unwrap();
        let parts = dfp
            .into_iter()
            .map(|x| {
                // let filters = x
                //     .get_column_names().into_iter().zip(x.get_row(0).unwrap().0.into_iter())
                //     .filter(|(k, _)| partitions.contains(&k.clone().to_string()))
                //     .map(|(k, v)| (k.clone().to_string(), v.get_str().unwrap_or("null")))
                //     .collect::<HashMap<String, String>>();
                // println!("{:?}", filters);
                DatasetPart::new(Some(x), None, None)
            }).collect::<Vec<DatasetPart>>();

        Self::new(Some(partitions.clone()), None, parts, storage)
    }

    pub fn from_dataframe_old(df: DataFrame, partitions: &Vec<String>, storage: Option<DatasetStorage>) -> Self {
        let start = SystemTime::now();
        let dfg = df.groupby(partitions).unwrap();
        let dfgg = dfg.groups().unwrap();
        println!("Group by took: {} ms", start.elapsed().unwrap().as_millis());

        let partition_values = partitions.iter().map(|p| {
            let c_arr = dfgg.column(p).unwrap().cast(&DataType::Utf8).unwrap();
            c_arr.utf8().unwrap().into_iter().flat_map(|x| x.into_iter()).map(|v| v.to_string()).collect::<Vec<String>>()
        }).collect::<Vec<Vec<String>>>();
        let values = (0..dfgg.height()).map(|i| (0..partitions.len()).map(|j| &partition_values[j][i]).collect()).collect::<Vec<Vec<&String>>>();

        let parts = values.into_iter().zip(dfgg.column("groups").unwrap().iter()).map(|(vals, idx)| {
            match idx {
                AnyValue::List(x) => {
                    let table = df.take(x.u32().unwrap()).unwrap();
                    let filters = partitions.clone().into_iter().zip(vals.into_iter().map(|v| v.clone())).collect::<HashMap<String, String>>();
                    DatasetPart::new(Some(table), Some(filters), None)
                }
                _ => panic!("Can only get List of u32 in groups")
            }
        }).collect::<Vec<DatasetPart>>();

        Self::new(Some(partitions.clone()), None, parts, storage)
    }

   
    // IO RELATED
    // pub fn from_storage(root: &String, lazy: bool) -> Self {
    //     let fpath = format!("{root}/manifest.json");
    //     let contents = std::fs::read_to_string(&fpath).expect("Could not read manifest file in given root");
    //     let mut obj = serde_json::from_str::<Self>(&contents).expect("Issue in deserialization of manifest");

    //     // Lazy load underlying parts
    //     let contains = "parquet".to_string();
    //     obj.parts = find_parts(&obj.storage.as_ref().unwrap().root, &contains, lazy);
    //     obj
    // }

    pub fn with_storage(mut self, storage: Option<DatasetStorage>) -> Self {
        self.storage = storage;
        self
    }

    pub fn to_storage(&self) {
        match &self.storage {
            Some(storage) => {
                // Create & clear directory
                fs::remove_dir_all(&storage.root).ok(); //#.expect("Remove files in root dir failed");
                fs::create_dir_all(&storage.root).expect("Create dir failed");

                // Save manifest
                let manifest_path = format!("{}/manifest.json", storage.root);
                fs::write(manifest_path, serde_json::to_string_pretty(&self).expect("Issue in serialization of manifest")).unwrap();

                // Save underlying parts
                let _ = self.parts
                    .par_iter()
                    .map(|p| {
                        match &p.table {
                            Some(t) => {
                                let partition_path = format!("{}/{}", storage.root, p.partition_path(&self.partitions));
                                fs::create_dir_all(&partition_path).expect("Create dir failed");
    
                                let file_path = format!("{}/{}/file.parquet", storage.root, p.partition_path(&self.partitions));
                                let file_pathbuf = PathBuf::from(file_path);
                                t.clone().lazy().sink_parquet(file_pathbuf, ParquetWriteOptions::default()).unwrap();
                            }
                            None => println!("Table has not been loaded yet!")
                        }
                    })
                    .collect::<()>();
            },
            None => println!("Storage options are not set on dataset")
        }
    }

}