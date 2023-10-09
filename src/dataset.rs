use polars::prelude::*;
use polars::prelude::LogicalPlan;
use rayon::prelude::*;
use std::path::Path;
use std::fs;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use anyhow::{Result, Error};

use serde_json;
use serde::{Serialize, Deserialize};

use crate::storage::{DatasetStorage, extract_files};
use crate::buckets::{expr_to_bucket, series_to_bucket};

#[derive(Clone)]
pub struct DatasetPart {
    table: Arc<Mutex<LazyFrame>>,
    partitions: Option<HashMap<String, String>>,
    bucket_by: Option<Vec<String>>,
    bucket_nr: Option<usize>,
    path: Option<String>,
    changes: Arc<Mutex<usize>>,
    rows: Arc<Mutex<usize>>,
}

pub fn rows_from_lazyframe(lf: &LazyFrame) -> usize {
    match &lf.logical_plan {
        LogicalPlan::DataFrameScan{df, ..} => { df.height() },
        _ => { 0 }
    }
}

impl DatasetPart {
    pub fn new( table: LazyFrame, partitions: Option<HashMap<String, String>>, bucket_by: Option<Vec<String>>, bucket_nr: Option<usize>, path: Option<String>) -> Self {
        let rows = Arc::new(Mutex::new(rows_from_lazyframe(&table)));
        let table = Arc::new(Mutex::new(table));
        let changes = Arc::new(Mutex::new(0));
        Self { table, partitions, bucket_by, bucket_nr, path, changes, rows }
    }

    pub fn with_root(mut self, root: &String) -> Self {
        self.path = Some(format!("{}/{}", root, self.file_path()));
        self
    }

    pub fn collect(&self) -> Result<&Self> {
        let mut lock = self.table.lock().unwrap();
        let df_new = lock.clone().collect()?.lazy();
        *lock = df_new;
        Ok(self)
    }

    pub fn schema(&self) -> Result<Arc<Schema>> {
        Ok(self.table.lock().unwrap().schema()?)
    }

    pub fn column_dtype(&self, column: &String) -> Result<DataType> {
        let schema = self.schema()?;
        Ok(schema.get(column).unwrap().clone())
    }

    pub fn to_lazyframe(&self, filter: bool) -> LazyFrame {
        let mut lf = self.table.lock().unwrap().clone();
        if filter {
            // if self.partitions.is_some() {
            //     for (k, v) in self.partitions.as_ref().unwrap().iter() {
            //         let _x = lit(v.clone());
            //         lf = lf.filter(col(k).eq(lit(v.into::<u32>()).cast(self.column_dtype(k).unwrap())));
            //     }
            // }
            if self.bucket_by.is_some() {
                let arr = expr_to_bucket(col(&self.bucket_by.as_ref().unwrap()[0]), self.column_dtype(&self.bucket_by.as_ref().unwrap()[0]).unwrap(), 5).unwrap();
                lf = lf.filter(arr.eq(lit(self.bucket_nr.unwrap() as u64)))
            }
            lf
        } else {
            lf
        }
    }

    pub fn insert(&self, other: DatasetPart, collect: bool) -> Result<&Self> {
        // Get write lock
        let mut t_lock = self.table.lock().unwrap();
        let mut c_lock = self.changes.lock().unwrap();
        let mut r_lock = self.rows.lock().unwrap();

        // Materialize both
        let left = t_lock.clone();
        let right = other.table.lock().unwrap().clone();
        let rows_changed = right.clone().collect()?.height();
        let df_new = concat([left, right], UnionArgs::default())?;

        // Replace in Mutex (collect if needed)
        if collect  { // | (c_lock.clone() + rows_changed > 10000)
            let df = df_new.collect()?;
            *r_lock = df.height();
            *t_lock = df.lazy();
            *c_lock = 0;
        } else {
            *t_lock = df_new; 
            *c_lock += rows_changed;   
            *r_lock += rows_changed;    
        };        
        Ok(self)
    }

    pub fn upsert(&self, other: DatasetPart, keys: Vec<String>, collect: bool) -> Result<&Self> {
        let keys_col = keys.iter().map(|c| col(c)).collect::<Vec<Expr>>();
        let coalesce_exp = self.schema().unwrap().iter_names()
            .map(|n| {
                if keys.contains(&n.as_str().to_string()) {
                    col(n)
                } else {
                    coalesce(&[col(&format!("{}_right", n)), col(n)]).alias(n)
                }
            })
            .collect::<Vec<Expr>>();

        // Get write lock
        let mut t_lock = self.table.lock().unwrap();
        let mut c_lock = self.changes.lock().unwrap();
        let mut r_lock = self.rows.lock().unwrap();

        // Materialize both
        let right = other.table.lock().unwrap().clone();

        // Outer join
        let df_outer = t_lock.clone().join(right.clone(), keys_col.clone(), keys_col.clone(), JoinArgs::new(JoinType::Outer));
        let df_new = df_outer.select(coalesce_exp);

        // Find rows which have changed
        let rows_changed = right.collect()?.height();

        // Replace in Mutex (collect if needed)
        if collect  { // | (c_lock.clone() + rows_changed > 10000)
            let df = df_new.collect()?;
            *r_lock = df.height();
            *t_lock = df.lazy();
            *c_lock = 0;
        } else {
            *t_lock = df_new; 
            *c_lock += rows_changed;   
            *r_lock += 0;    
        };        
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
        let mut df = self.table.lock().unwrap().clone().collect()?;
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
    pub parts: Arc<Mutex<HashMap<String, DatasetPart>>>, // Underlying parts (referencing to tables)
    pub storage: Option<DatasetStorage> // Storage options
}

impl Dataset {
    pub fn new(partitions: Option<Vec<String>>, buckets: Option<Vec<String>>, parts: Arc<Mutex<HashMap<String, DatasetPart>>>, storage: Option<DatasetStorage>) -> Self {
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
        let dfp = if group_cols.len() > 0 {
            df.partition_by(group_cols, true)?
        } else {
            vec![df]
        };
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

        Ok(Self::new(partitions.clone(), buckets, Arc::new(Mutex::new(parts)), storage))
    }

    pub fn to_lazyframe(&self) -> Result<LazyFrame> {
        let frames = self.parts.lock().unwrap().iter().map(|(_,f)| f.to_lazyframe(true)).collect::<Vec<LazyFrame>>();
        Ok(concat(frames, UnionArgs::default())?)
    }

    pub fn rows(&self) -> usize {
        let rows = self.parts.lock().unwrap()
            .par_iter()
            .map(|(_, p)| {
                p.rows.lock().unwrap().clone()
            })
            .sum();
        rows
    }

    pub fn schema(&self) -> Result<Arc<Schema>> {
        let parts = self.parts.lock().unwrap();
        parts.values().next().ok_or(Error::msg("No parts"))?.schema()
    }

    pub fn collect(&self) -> Result<()> {
        let _ = self.parts.lock().unwrap()
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
        let lock = other.parts.lock().unwrap();
        let _ = lock
            .iter()
            .map(|(other_path, other_part)| {
                let mut parts = self.parts.lock().unwrap();
                match parts.get(other_path) {
                    Some(sp) => {
                        sp.insert(other_part.clone(), false).unwrap();
                    }, 
                    None => {
                        parts.insert(other_path.clone(), other_part.clone());
                    }
                }
                other_path.clone()
            })
            .collect::<Vec<String>>();
        
        if save { self.to_storage()? }
        Ok(())
    }


    pub fn upsert(&self, df: DataFrame, keys: Vec<String>, save: bool) -> Result<()> {
        // Split dataframe into dataset
        let other = Dataset::from_dataframe(df, self.partitions.clone(), self.buckets.clone(), self.storage.clone())?;

        // Upsert into parts and keep paths which are changes for saving
        let lock = other.parts.lock().unwrap();
        let _ = lock
            .iter()
            .map(|(other_path, other_part)| {
                let mut parts = self.parts.lock().unwrap();
                match parts.get(other_path) {
                    Some(sp) => {
                        sp.upsert(other_part.clone(), keys.clone(), false).unwrap();
                    }, 
                    None => {
                        parts.insert(other_path.clone(), other_part.clone());
                    }
                }
                other_path.clone()
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
                let _ = self.parts.lock().unwrap()
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
            
                obj.parts = Arc::new(Mutex::new(root_files 
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
                    .collect::<HashMap<String, DatasetPart>>()));
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