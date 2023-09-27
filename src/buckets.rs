use polars::prelude::*;
use polars::export::ahash::RandomState;
use anyhow::{anyhow, Result};

// Cast to categorical: df.replace("option_name", df.column("option_name").unwrap().cast(&DataType::Utf8).unwrap().cast(&DataType::Categorical(None)).unwrap()).unwrap();

// Presto bucketing 000234_0_20180102_030405_00641_x1y2z
pub fn series_to_bucket(arr: &Series, buckets: u32) -> Result<Series> {
    match arr.dtype() {
        DataType::Boolean => Ok(arr % 1),
        DataType::UInt8  => Ok(arr % buckets),
        DataType::UInt16 => Ok(arr % buckets),
        DataType::UInt32 => Ok(arr % buckets),
        DataType::UInt64 => Ok(arr % buckets),
        DataType::Int8   => Ok(arr % buckets),
        DataType::Int16  => Ok(arr % buckets),
        DataType::Int32  => Ok(arr % buckets),
        DataType::Int64  => Ok(arr % buckets),
        DataType::Float32 => Ok(arr % buckets),
        DataType::Float64 => Ok(arr % buckets),
        DataType::Utf8    => {
            let rs = RandomState::new();
            let mut buf = Vec::new();
            arr.utf8().unwrap().vec_hash(rs, &mut buf)?;
            Ok(Series::new(arr.name(), &buf))
        },
        DataType::Date => Ok(arr % buckets),
        _ => Err(anyhow!("Invalid data type for bucketing: {}", arr.dtype()))
    }
}

pub fn expr_to_bucket(arr: Expr, dtype: DataType, buckets: u32) -> Result<Expr> {
    match dtype {
        DataType::Boolean => Ok(arr % lit(buckets)),
        DataType::UInt8  => Ok(arr % lit(buckets)),
        DataType::UInt16 => Ok(arr % lit(buckets)),
        DataType::UInt32 => Ok(arr % lit(buckets)),
        DataType::UInt64 => Ok(arr % lit(buckets)),
        DataType::Int8   => Ok(arr % lit(buckets)),
        DataType::Int16  => Ok(arr % lit(buckets)),
        DataType::Int32  => Ok(arr % lit(buckets)),
        DataType::Int64  => Ok(arr % lit(buckets)),
        DataType::Float32 => Ok(arr % lit(buckets)),
        DataType::Float64 => Ok(arr % lit(buckets)),
        // DataType::Utf8    => {
        //     let rs = RandomState::new();
        //     let mut buf = Vec::new();
        //     arr.utf8().unwrap().vec_hash(rs, &mut buf)?;
        //     Ok(Series::new(arr.name(), &buf))
        // },
        DataType::Date => Ok(arr % lit(buckets)),
        _ => Err(anyhow!("Invalid data type for bucketing: {}", dtype))
    }
}