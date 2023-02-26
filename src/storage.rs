use std::path::Path;
use std::fs;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Format {
    Parquet,
    Ipc,
    Csv,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Compression {
    Snappy,
    // Gzip,
    // Lzo,
    // Brotli,
    // Lz4,
    // Zstd,
    Lz4Raw,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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

pub fn extract_files<'a>(dir: &Path, contains: &String, files: &'a mut Vec<String>) -> &'a mut Vec<String> {
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
