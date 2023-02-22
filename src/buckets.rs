use polars::prelude::*;
use polars::export::ahash::RandomState;

pub fn series_to_bucket(arr: &Series, buckets: u32) -> Series {
    match arr.dtype() {
        DataType::Boolean => arr % 1,
        DataType::UInt8  => arr % buckets,
        DataType::UInt16 => arr % buckets,
        DataType::UInt32 => arr % buckets,
        DataType::UInt64 => arr % buckets,
        DataType::Int8   => arr % buckets,
        DataType::Int16  => arr % buckets,
        DataType::Int32  => arr % buckets,
        DataType::Int64  => arr % buckets,
        DataType::Float32 => arr % buckets,
        DataType::Float64 => arr % buckets,
        DataType::Utf8    => {
            let rs = RandomState::new();
            let mut buf = Vec::new();
            arr.utf8().unwrap().vec_hash(rs, &mut buf);
            Series::new(arr.name(), &buf)
        },
        DataType::Date => arr % buckets,
        _ => panic!("Hash is not implemented for {}", arr.dtype())
    }
}