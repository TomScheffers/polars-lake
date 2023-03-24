use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use anyhow::Result;

use arrow2::io::ipc::read;
use polars::frame::DataFrame;
use polars::ipc::reader;
use arrow2::io::ipc::read::FileReader;

fn main() -> Result<()> {
    const ADDRESS: &str = "127.0.0.1:12989";

    let mut reader = TcpStream::connect(ADDRESS)?;
    let metadata = read::read_stream_metadata(&mut reader)?;
    let fields = (&metadata).schema.fields.clone();
    let mut stream = read::StreamReader::new(&mut reader, metadata, None);

    let mut idx = 0;
    loop {
        match stream.next() {
            Some(x) => match x {
                Ok(read::StreamState::Some(b)) => {
                    idx += 1;
                    let df = DataFrame::try_from((b, fields.as_slice()))?;
                    println!("batch: {:?}, {:?}", idx, df)
                }
                Ok(read::StreamState::Waiting) => thread::sleep(Duration::from_millis(100)),
                Err(l) => println!("{:?} ({})", l, idx),
            },
            None => break,
        };
    }

    Ok(())
}