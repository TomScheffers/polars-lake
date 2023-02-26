use polars::prelude::*;
use rayon::prelude::*;
use std::path::Path;
use std::path::PathBuf;
use std::fs;
use std::collections::HashMap;

use serde_json;
use serde::{Serialize, Deserialize};

use crate::storage::{DatasetStorage, extract_files};
use crate::buckets::series_to_bucket;

pub enum Frame {
    DataFrame(DataFrame),
    LazyFrame(LazyFrame)
}

pub struct DatasetPart {
    table: LazyFrame,
    partitions: Option<HashMap<String, String>>,
    bucket_by: Option<Vec<String>>,
    bucket_nr: Option<usize>,
    path: Option<String>,
}

impl DatasetPart {
    pub fn new( table: LazyFrame, partitions: Option<HashMap<String, String>>, bucket_by: Option<Vec<String>>, bucket_nr: Option<usize>, path: Option<String>) -> Self {
        Self { table, partitions, bucket_by, bucket_nr, path }
    }

    pub fn load(self) -> Self {
        self
    }

    pub fn partition_path(&self) -> String {
        match &self.partitions {
            Some(partitions) => {
                let mut parts = Vec::new();
                for (k, v) in partitions.iter() {
                    parts.push(format!("{}={}", k, v));
                }
                parts.join("/")
            },
            None => "".to_string()
        } 
    }

    pub fn path(&self) -> String {
        let folder = self.partition_path();
        match self.bucket_nr {
            Some(bn) => format!("{}/{:0>6}_file.parquet", folder, bn),
            None => format!("{}/file.parquet", folder),
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

    pub fn from_dataframe(mut df: DataFrame, partitions: Option<Vec<String>>, buckets: Option<Vec<String>>, storage: Option<DatasetStorage>) -> Self {
        // Compute hash buckets if needed
        let mut group_cols = partitions.clone().unwrap_or(Vec::new());
        if let Some(ref bb) = buckets {
            let mut arr = series_to_bucket(df.column(&bb[0]).unwrap(), 5);
            arr.rename("$bucket");
            df.with_column(arr).unwrap();
            group_cols.push("$bucket".to_string());
        }
        
        // Partition by
        let dfp = df.partition_by(group_cols).unwrap();
        let parts = dfp
            .into_iter()
            .map(|x| {
                let partition_values = match &partitions {
                    Some(parts) => parts.iter().map(|p| (p.clone(), format!("{}", x.column(p).unwrap().get(0).unwrap()))).collect::<HashMap<String, String>>(),
                    None => HashMap::new()
                };
                let bucket_nr = match &buckets {
                    Some(_) => {
                        let val = x.column("$bucket").unwrap().get(0).unwrap();
                        match val {
                            AnyValue::Int64(x) => Some(x as usize),
                            _ => Some(0)                            
                        }
                    },
                    None => None
                };
                DatasetPart::new(x.lazy(), Some(partition_values), buckets.clone(), bucket_nr, None)
            }).collect::<Vec<DatasetPart>>();

        Self::new(partitions.clone(), buckets, parts, storage)
    }

    pub fn upsert(self, df: DataFrame) {
        // Split dataframe into dataset
        let other = Dataset::from_dataframe(df, self.partitions.clone(), self.buckets.clone(), self.storage.clone());

        // Align parts of self & other
        let _part_map = &other.parts
            .iter()
            .map(|other_part| {
                let matches = self.parts.iter().enumerate().filter(|(_, self_parts)| (other_part.partitions == self_parts.partitions) & (other_part.bucket_nr == self_parts.bucket_nr)).map(|(i, _)| i).collect::<Vec<usize>>();
                if matches.len() > 0 {
                    Some(matches[0])
                } else {
                    None                    
                }
            })
            .collect::<Vec<Option<usize>>>();
    }
   
    // IO RELATED
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
                        if let Some(_) = &self.partitions {
                            let partition_path = format!("{}/{}", storage.root, p.partition_path());
                            fs::create_dir_all(&partition_path).expect("Create dir failed");
                        }
                        let file_path = format!("{}/{}", storage.root, p.path());
                        let file_pathbuf = PathBuf::from(file_path);
                        p.table.clone().sink_parquet(file_pathbuf, ParquetWriteOptions::default()).unwrap();
                    })
                    .collect::<()>();
            },
            None => println!("Storage options are not set on dataset")
        }
    }

    pub fn from_storage(root: &String) -> Self {
        let manifest_path = format!("{root}/manifest.json");
        let contents = std::fs::read_to_string(&manifest_path).expect("Could not read manifest file in given root");
        let mut obj = serde_json::from_str::<Self>(&contents).expect("Issue in deserialization of manifest");

        // Load underlying parts\
        let contains = ".parquet".to_string();
        match obj.storage {
            Some(ref storage) => {
                let mut empty = Vec::new();
                let root_files = extract_files(&Path::new(&storage.root), &contains, &mut empty);
            
                obj.parts = root_files 
                    .par_iter()
                    .map(|path| {
                        let mut partitions = HashMap::new();
                        for v in path.split("/").collect::<Vec<&str>>() {
                            if v.contains("=") {
                                let arr = v.split("=").collect::<Vec<&str>>();
                                partitions.insert(arr[0].to_string(), arr[1].to_string());
                            }
                        }
                        
                        let bucket_by = &obj.buckets;
                        let bucket_nr = match obj.buckets {
                            Some(_) => {
                                let mut i = 0;
                                loop {
                                    if path.chars().skip(storage.root.len() + 1).nth(i).unwrap() != '0' {
                                        break Some(path.chars().skip(storage.root.len() + 1 + i).take(6 - i).collect::<String>().parse::<usize>().unwrap()) //path[i..6].parse::<usize>().unwrap())
                                    }
                                    i += 1;
                                    if i == 6 {
                                        break Some(0)
                                    }
                                }
                            },
                            None => None
                        };
                        let table = LazyFrame::scan_parquet(path, ScanArgsParquet::default()).expect("Cannot read parquet file");
                        DatasetPart::new(table, Some(partitions), bucket_by.clone(), bucket_nr, Some(path.clone()))
                    })
                    .collect::<Vec<DatasetPart>>();
            },
            None => println!("Cannot load parts because StorageOptions are not present")
        };
        obj
    }
}