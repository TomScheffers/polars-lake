use polars::prelude::*;
use rayon::prelude::*;
use std::path::Path;
use std::fs;
use std::collections::HashMap;
use std::sync::RwLock;
use anyhow::Result;

use serde_json;
use serde::{Serialize, Deserialize};

use crate::storage::{DatasetStorage, extract_files};
use crate::buckets::series_to_bucket;

pub enum Frame {
    DataFrame(DataFrame),
    LazyFrame(LazyFrame)
}

pub struct DatasetPart {
    table: RwLock<LazyFrame>,
    partitions: Option<HashMap<String, String>>,
    bucket_by: Option<Vec<String>>,
    bucket_nr: Option<usize>,
    path: Option<String>,
    changes: RwLock<usize>,
}

impl DatasetPart {
    pub fn new( table: LazyFrame, partitions: Option<HashMap<String, String>>, bucket_by: Option<Vec<String>>, bucket_nr: Option<usize>, path: Option<String>) -> Self {
        let table = RwLock::new(table);
        let changes = RwLock::new(0);
        Self { table, partitions, bucket_by, bucket_nr, path, changes }
    }

    pub fn with_root(mut self, root: &String) -> Self {
        self.path = Some(format!("{}/{}", root, self.file_path()));
        self
    }

    pub fn collect(&self) -> Result<&Self> {
        let df_new = self.table.read().unwrap().clone().collect()?.lazy();
        *self.table.write().unwrap() = df_new;
        Ok(self)
    }

    pub fn insert(&self, other: DatasetPart, collect: bool) -> Result<&Self> {
        // Materialize both
        let left = self.table.read().unwrap().clone(); //.cache();
        let right = other.table.read().unwrap().clone(); //.cache();
        let rows_changed = right.clone().collect()?.height();
        let df_new = concat([left, right], UnionArgs::default())?;

        // Replace in Mutex (collect if needed)
        if collect | (self.changes.read().unwrap().clone() + rows_changed > 10000) {
            let df = df_new.collect()?.lazy();
            *self.table.write().unwrap() = df;
            *self.changes.write().unwrap() = 0;
        } else {
            *self.table.write().unwrap() = df_new; 
            *self.changes.write().unwrap() += rows_changed;       
        };        
        Ok(self)
    }

    pub fn upsert(&self, other: DatasetPart, keys: Vec<String>, collect: bool) -> Result<&Self> {
        let keys_col = keys.iter().map(|c| col(c)).collect::<Vec<Expr>>();
        let coalesce_exp = self.table.read().unwrap().schema().unwrap().iter_names()
            .map(|n| {
                if keys.contains(&n.as_str().to_string()) {
                    col(n)
                } else {
                    coalesce(&[col(&format!("{}_right", n)), col(n)]).alias(n)
                }
            })
            .collect::<Vec<Expr>>();

        // Materialize both
        let left = self.table.read().unwrap().clone(); //.cache();
        let right = other.table.read().unwrap().clone(); //.cache();

        // Outer join
        let df_outer = left.clone().join(right.clone(), keys_col.clone(), keys_col.clone(), JoinArgs::new(JoinType::Outer));
        let df_new = df_outer.select(coalesce_exp);

        // Find rows which have changed
        let rows_changed = right.collect()?.height();

        // Replace in Mutex (collect if needed)
        if collect | (self.changes.read().unwrap().clone() + rows_changed > 10000) {
            let df = df_new.collect()?.lazy();
            *self.table.write().unwrap() = df;
            *self.changes.write().unwrap() = 0;
        } else {
            // let mut lock = self.table.write().unwrap();
            *self.table.write().unwrap() = df_new; 
            *self.changes.write().unwrap() += rows_changed;       
        };
        println!("Rows changed counter: {:?}", self.changes.read().unwrap().clone());
        
        Ok(self)
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

    pub fn save(&self, root: &String) -> Result<()> {
        let partition_root = format!("{}/{}", root, self.partition_path());
        fs::create_dir_all(&partition_root)?;
        let path = format!("{}/{}", root, self.file_path());
        let mut file = std::io::BufWriter::new(std::fs::File::create(&path)?);
        let mut df = self.table.read().unwrap().clone().collect()?;
        println!("Saving df of size {:?} in path {:?}", df.shape(), path);
        ParquetWriter::new(&mut file).finish(&mut df)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
pub struct Dataset {
    pub partitions: Option<Vec<String>>, // File based partitioning columns
    pub buckets: Option<Vec<String>>, // Hash bucketing columns (within partitions)
    #[serde(skip_serializing, skip_deserializing)]
    pub parts: RwLock<HashMap<String, DatasetPart>>, // Underlying parts (referencing to tables)
    pub storage: Option<DatasetStorage> // Storage options
}

impl Dataset {
    pub fn new(partitions: Option<Vec<String>>, buckets: Option<Vec<String>>, parts: RwLock<HashMap<String, DatasetPart>>, storage: Option<DatasetStorage>) -> Self {
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
        let dfp = df.partition_by(group_cols, true)?;
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
                let path = part.file_path();
                match &storage {
                    Some(s) => (path, part.with_root(&s.root)),
                    None => (path, part)
                }
            }).collect::<HashMap<String, DatasetPart>>();

        Ok(Self::new(partitions.clone(), buckets, RwLock::new(parts), storage))
    }

    pub fn to_lazyframe(&self) -> Result<LazyFrame> {
        let frames = self.parts.read().unwrap().iter().map(|(_,f)| f.table.read().unwrap().clone()).collect::<Vec<LazyFrame>>();
        Ok(concat(frames, UnionArgs::default())?)
    }

    pub fn collect(&self) -> Result<()> {
        let _ = self.parts.read().unwrap()
            .par_iter()
            .map(|(_, p)| {
                p.collect().unwrap();
                None
            })
            .collect::<Vec<Option<()>>>();
        Ok(())
    }

    pub fn insert(&self, df: DataFrame, save: bool) -> Result<()> {
        // Split dataframe into dataset
        let other = Dataset::from_dataframe(df, self.partitions.clone(), self.buckets.clone(), self.storage.clone())?;

        // Insert into parts and keep paths which are changes for saving
        let _ = other.parts.into_inner().unwrap()
            .into_iter()
            .map(|(other_path, other_part)| {
                let parts = self.parts.read().unwrap();
                match parts.get(&other_path) {
                    Some(sp) => {
                        sp.insert(other_part, false).unwrap();
                    }, 
                    None => {
                        self.parts.write().unwrap().insert(other_path.clone(), other_part);
                    }
                }
                other_path
            })
            .collect::<Vec<String>>();
        
        if save { self.to_storage()? }
        Ok(())
    }


    pub fn upsert(&self, df: DataFrame, keys: Vec<String>, save: bool) -> Result<()> {
        // Split dataframe into dataset
        let other = Dataset::from_dataframe(df, self.partitions.clone(), self.buckets.clone(), self.storage.clone())?;

        // Upsert into parts and keep paths which are changes for saving
        let _ = other.parts.into_inner().unwrap()
            .into_iter()
            .map(|(other_path, other_part)| {
                let parts = self.parts.read().unwrap();
                match parts.get(&other_path) {
                    Some(sp) => {
                        sp.upsert(other_part, keys.clone(), false).unwrap();
                    }, 
                    None => {
                        self.parts.write().unwrap().insert(other_path.clone(), other_part);
                    }
                }
                other_path
            })
            .collect::<Vec<String>>();
        
        if save { self.to_storage()? }
        Ok(())
    }
   
    // IO RELATED
    pub fn with_storage(mut self, storage: Option<DatasetStorage>) -> Self {
        self.storage = storage;
        self
    }

    pub fn to_storage(&self) -> Result<()> {
        match &self.storage {
            Some(storage) => {
                // Create & clear directory
                fs::remove_dir_all(&storage.root).ok(); //#.expect("Remove files in root dir failed");
                fs::create_dir_all(&storage.root).expect("Create dir failed");

                // Save manifest
                let manifest_path = format!("{}/manifest.json", storage.root);
                fs::write(manifest_path, serde_json::to_string_pretty(&self).expect("Issue in serialization of manifest")).unwrap();

                // Save underlying parts
                let _ = self.parts.read().unwrap()
                    .par_iter()
                    .map(|(_, p)| {
                        p.save(&storage.root)?;
                        Ok(())
                    })
                    .collect::<Result<()>>();
                Ok(())
            },
            None => Ok(()), //TODO: Yield error
        }
    }

    pub fn from_storage(root: &String, load: bool) -> Result<Self> {
        let manifest_path = format!("{root}/manifest.json");
        let contents = std::fs::read_to_string(&manifest_path)?;
        let mut obj = serde_json::from_str::<Self>(&contents)?;

        // Load underlying parts\
        let contains = ".parquet".to_string();
        match obj.storage {
            Some(ref storage) => {
                let mut empty = Vec::new();
                let root_files = extract_files(&Path::new(&storage.root), &contains, &mut empty);
            
                obj.parts = RwLock::new(root_files 
                    .par_iter()
                    .map(|path| {
                        let mut partitions = HashMap::new();
                        for v in path.split("/").collect::<Vec<&str>>() {
                            if v.contains("=") {
                                let arr = v.split("=").collect::<Vec<&str>>();
                                partitions.insert(arr[0].to_string(), arr[1].to_string());
                            }
                        }
                        
                        let file_name = path.split("/").last().unwrap();
                        let bucket_by = &obj.buckets;
                        let bucket_nr = match obj.buckets {
                            Some(_) => {
                                let mut i = 0;
                                loop {
                                    if file_name.chars().nth(i).unwrap() != '0' {
                                        break Some(file_name.chars().skip(i).take(6 - i).collect::<String>().parse::<usize>().unwrap()) //path[i..6].parse::<usize>().unwrap())
                                    }
                                    i += 1;
                                    if i == 6 {
                                        break Some(0)
                                    }
                                }
                            },
                            None => None
                        };
                        let table = if load {
                            LazyFrame::scan_parquet(path, ScanArgsParquet::default()).unwrap().collect().unwrap().lazy()
                        } else {
                            LazyFrame::scan_parquet(path, ScanArgsParquet::default()).unwrap()
                        };
                        let part = DatasetPart::new(table, Some(partitions), bucket_by.clone(), bucket_nr, Some(path.clone()));
                        let path = part.file_path();
                        (path, part)
                    })
                    .collect::<HashMap<String, DatasetPart>>());
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

    #[test]
    fn create_dataset() -> Result<()> {
        let start = SystemTime::now();
        let df = LazyFrame::scan_parquet("data/stock_current/org_key=1/file.parquet", ScanArgsParquet::default())?.with_column(lit(1).alias("org_key")).collect()?; 
        println!("Reading table took: {} ms. Rows {:?}", start.elapsed().unwrap().as_millis(), df.shape());
    
        // Dataset from DataFrame
        let start = SystemTime::now();
        let parts = vec!["org_key".to_string()];
        let buckets = vec!["sku_key".to_string()];
        let ds = Dataset::from_dataframe(df.clone(), Some(parts), Some(buckets), None)?;
        println!("Creating dataset from dataframe took: {} ms.", start.elapsed().unwrap().as_millis());
    
        let start = SystemTime::now();
        let store = DatasetStorage::new("data/stock_parts".to_string(), Format::Parquet, Some(Compression::Snappy));
        let ds = ds.with_storage(Some(store));
        ds.to_storage()?;
        println!("Saving dataset took: {} ms", start.elapsed().unwrap().as_millis());

        let start = SystemTime::now();
        let ds = Dataset::from_storage(&"data/stock_parts".to_string(), false)?; // ("data/stock_current/org_key=1/file.parquet", args).unwrap().collect().unwrap(); 
        println!("Reading dataset took: {} ms.", start.elapsed().unwrap().as_millis());
    
        let start = SystemTime::now();
        let dfu = df.head(Some(300000));
        let keys = vec!["store_key".to_string(), "sku_key".to_string()];
        ds.upsert(dfu, keys, true)?;
        println!("Upsert table took: {} ms.", start.elapsed().unwrap().as_millis());

        let start = SystemTime::now();
        let dfu = df.head(Some(1));
        let keys = vec!["store_key".to_string(), "sku_key".to_string()];
        ds.upsert(dfu, keys, false)?;
        println!("Upsert table took: {} ms.", start.elapsed().unwrap().as_millis());

        Ok(())
    }
}