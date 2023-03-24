use polars::prelude::*;
use rayon::prelude::*;
use std::path::Path;
use std::fs;
use std::collections::HashMap;
use anyhow::Result;

use serde_json;
use serde::{Serialize, Deserialize};

use crate::storage::{DatasetStorage, extract_files};
use crate::buckets::series_to_bucket;

pub enum Frame {
    DataFrame(DataFrame),
    LazyFrame(LazyFrame)
}

#[derive(Clone)]
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

    pub fn with_root(mut self, root: &String) -> Self {
        self.path = Some(format!("{}/{}", root, self.file_path()));
        self
    }

    pub fn upsert(&self, other: DatasetPart, keys: Vec<String>) -> Result<Self> {
        let keys_col = keys.iter().map(|c| col(c)).collect::<Vec<Expr>>();
        let coalesce_exp = self.table.schema().unwrap().iter_names()
            .map(|n| {
                if keys.contains(n) {
                    col(n)
                } else {
                    coalesce(&[col(&format!("{}_right", n)), col(n)]).alias(n)
                }
            })
            .collect::<Vec<Expr>>();

        // Materialize both
        let left = self.table.clone().cache();
        let right = other.table.clone().cache();

        // Coalesce matches
        let df_left = left.clone().join(right.clone(), keys_col.clone(), keys_col.clone(), JoinType::Left);
        let df_match = df_left.select(coalesce_exp);

        // Only left or right
        let df_no_match = right.clone().join(left.clone(), keys_col.clone(), keys_col.clone(), JoinType::Anti);

        let df_new = concat([df_match, df_no_match], true, true)?;

        let df_final = df_new.collect()?.lazy();

        Ok(Self { table: df_final, partitions: self.partitions.clone(), bucket_by: self.bucket_by.clone(), bucket_nr: self.bucket_nr, path: self.path.clone()})
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

    pub fn file_path(&self) -> String {
        let folder = self.partition_path();
        match self.bucket_nr {
            Some(bn) => format!("{}/{:0>6}_file.parquet", folder, bn),
            None => format!("{}/file.parquet", folder),
        }
    }

    pub fn save(&self) -> Result<()> {
        match &self.path {
            Some(p) => {
                let mut df = self.table.clone().collect()?;
                let mut file = std::io::BufWriter::new(std::fs::File::create(p)?);
                println!("Saving df of size {:?}", df.shape());
                ParquetWriter::new(&mut file).finish(&mut df)?;
            },
            None => println!("Cannnot save as path is not set")
        };
        Ok(())
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

    pub fn from_dataframe(mut df: DataFrame, partitions: Option<Vec<String>>, buckets: Option<Vec<String>>, storage: Option<DatasetStorage>) -> Result<Self> {
        // Compute hash buckets if needed
        let mut group_cols = partitions.clone().unwrap_or(Vec::new());
        if let Some(ref bb) = buckets {
            let mut arr = series_to_bucket(df.column(&bb[0]).unwrap(), 5)?;
            arr.rename("$bucket");
            df.with_column(arr)?;
            group_cols.push("$bucket".to_string());
        }
        
        // Partition by
        let dfp = df.partition_by(group_cols)?;
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
                let part = DatasetPart::new(x.lazy(), Some(partition_values), buckets.clone(), bucket_nr, None);
                match &storage {
                    Some(s) => part.with_root(&s.root),
                    None => part
                }
            }).collect::<Vec<DatasetPart>>();

        Ok(Self::new(partitions.clone(), buckets, parts, storage))
    }

    pub fn upsert(&mut self, df: DataFrame, keys: Vec<String>, save: bool) -> Result<Self> {
        // Split dataframe into dataset
        let other = Dataset::from_dataframe(df, self.partitions.clone(), self.buckets.clone(), self.storage.clone())?;

        // Align parts of self & other
        let part_map = &other.parts
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

        let mut new_parts = other.parts
            .into_iter()
            .zip(part_map)
            .map(|(other_part, sidx)| {
                match sidx {
                    Some(idx) => self.parts.get(*idx).unwrap().upsert(other_part, keys.clone()).unwrap(),
                    None => other_part,
                }
            })
            .collect::<Vec<DatasetPart>>();
        
        if save {
            let _ = &new_parts.par_iter().map(|p| p.save()).collect::<Vec<Result<()>>>();
        }

        // Combine parts
        let untouched_parts = self.parts.iter().enumerate().filter(|(i, _)| !part_map.contains(&Some(*i))).map(|(_, p)| p.clone()).collect::<Vec<DatasetPart>>();
        new_parts.extend(untouched_parts);

        Ok(Self::new(self.partitions.clone(), self.buckets.clone(), new_parts, self.storage.clone()))
    }
   
    // IO RELATED
    pub fn with_storage(mut self, storage: Option<DatasetStorage>) -> Self {
        self.storage = storage;
        self
    }

    pub fn to_storage(self) {
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
                    .into_par_iter()
                    .map(|p| {
                        let p = p.with_root(&storage.root);
                        match p.save() {
                            Ok(_) => {},
                            Err(e) => println!("Saving failed: {:?}", e)
                        };
                    })
                    .collect::<()>();
            },
            None => println!("Storage options are not set on dataset")
        }
    }

    pub fn from_storage(root: &String) -> Result<Self> {
        let manifest_path = format!("{root}/manifest.json");
        let contents = std::fs::read_to_string(&manifest_path)?;
        let mut obj = serde_json::from_str::<Self>(&contents)?;

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
                        let table = LazyFrame::scan_parquet(path, ScanArgsParquet::default()).unwrap();
                        DatasetPart::new(table, Some(partitions), bucket_by.clone(), bucket_nr, Some(path.clone()))
                    })
                    .collect::<Vec<DatasetPart>>();
            },
            None => println!("Cannot load parts because StorageOptions are not present")
        };
        Ok(obj)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::*;
    use std::time::SystemTime;
    use polars::toggle_string_cache;

    #[test]
    fn create_dataset() -> Result<()> {
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
        let mut ds = Dataset::from_storage(&"data/stock_parts".to_string())?; // ("data/stock_current/org_key=1/file.parquet", args).unwrap().collect().unwrap(); 
        println!("Reading dataset took: {} ms.", start.elapsed().unwrap().as_millis());
    
        let start = SystemTime::now();
        let args = ScanArgsParquet::default();
        let df = LazyFrame::scan_parquet("data/stock_current/org_key=1/file.parquet", args)?.collect()?; 
        println!("Reading table took: {} ms. Rows {:?}", start.elapsed().unwrap().as_millis(), df.shape());
    
        let start = SystemTime::now();
        let dfu = df.head(Some(10000));
        let keys = vec!["store_key".to_string(), "sku_key".to_string()];
        let mut ds = ds.upsert(dfu, keys, true)?;
        println!("Upsert table took: {} ms.", start.elapsed().unwrap().as_millis());

        let start = SystemTime::now();
        let dfu = df.head(Some(1));
        let keys = vec!["store_key".to_string(), "sku_key".to_string()];
        let _ = ds.upsert(dfu, keys, false)?;
        println!("Upsert table took: {} ms.", start.elapsed().unwrap().as_millis());

        Ok(())
    }
}