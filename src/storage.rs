use std::path::Path;
use std::fs;
use std::collections::HashMap;
use rayon::prelude::*;
use serde::{Serialize, Deserialize};

use crate::dataset::DatasetPart;

#[derive(Serialize, Deserialize, Debug)]
pub enum Format {
    Parquet,
    Ipc,
    Csv,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Compression {
    Snappy,
    // Gzip,
    // Lzo,
    // Brotli,
    // Lz4,
    // Zstd,
    Lz4Raw,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DatasetStorage {
    pub root: String, // Root folder (relative to code)
    pub format: Format,
    pub compression: Option<Compression>,
    // versioned: Bool,
    // acid: Option<Bool>
}

impl DatasetStorage {
    pub fn new( root: String, format: Format, compression: Option<Compression>) -> Self {
        Self { root, format, compression }
    }
}

fn extract_files<'a>(dir: &Path, contains: &String, files: &'a mut Vec<String>) -> &'a mut Vec<String> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                extract_files(&path, contains, files);
            } else {
                let file = path.to_str().unwrap().to_string();
                if file.contains(contains) {
                    files.push(file);
                }
            }
        }
    }
    files
}

fn find_parts(root: &String, contains: &String, lazy: bool) -> Vec<DatasetPart> {
    let mut empty = Vec::new();
    let root_files = extract_files(&Path::new(root), contains, &mut empty);

    let parts = root_files 
        .par_iter()
        .map(|path| {
            let mut filters = HashMap::new();
            for v in path.split("/").collect::<Vec<&str>>() {
                if v.contains("=") {
                    let arr = v.split("=").collect::<Vec<&str>>();
                    filters.insert(arr[0].to_string(), arr[1].to_string());
                }
            }
            let part = DatasetPart::new(None, Some(filters), Some(path.clone()));
            if lazy {
                part
            } else {
                part.load()
            }
        })
        .collect::<Vec<DatasetPart>>();
    parts
}
