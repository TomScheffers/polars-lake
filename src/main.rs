#![allow(dead_code)]

use anyhow::Result;

mod dataset;
mod storage;
mod buckets;

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
    Ok(())
}
