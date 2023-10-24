import grpc, concurrent, time, os
import proto.db_pb2 as pb2
import proto.db_pb2_grpc as pb2_grpc
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import io, socket, time
import numpy as np

# python -m grpc_tools.protoc --proto_path=. ./proto/db.proto --python_out=. --grpc_python_out=.

root = "data/taxi/"
schema, frames = None, []
for p in os.listdir(root):
    frame = pl.scan_parquet(root + p).with_columns(pl.col("tpep_pickup_datetime").dt.date().alias("date"))
    if "Airport_fee" in frame.columns: frame = frame.drop("Airport_fee")
    if "airport_fee" in frame.columns: frame = frame.drop("airport_fee")
    if schema:
        frame = frame.cast({k:v for k,v in schema.items() if k in frame.columns}, strict=False)
    else:
        schema = frame.schema
    frames.append(frame)
t = pl.concat(frames).collect()

def table_to_ipc(frame):
    b = io.BytesIO()
    frame.write_ipc(b)
    b.seek(0)
    return b.read()

def table_generator(size=25_000, start_idx=0, end_idx=None):
    for ts in t.slice(start_idx, end_idx).iter_slices(size):
        yield pb2.SourceIpc(schema="public", table="taxi", data=table_to_ipc(ts), partitions=[], buckets=[], keys=[])

# Create table
with grpc.insecure_channel("localhost:50051") as channel:
    stub = pb2_grpc.DbStub(channel)

    t1 = time.time()
    m = stub.CreateTable(table_generator(size=25_000, start_idx=0, end_idx=50_000))
    print("Create (Stream)", time.time() - t1)

    t1 = time.time()
    m = stub.InsertTable(table_generator(size=25_000, start_idx=50_000, end_idx=None))
    print("Insert (Stream)", time.time() - t1)

    t1 = time.time()
    m = stub.MaterializeTable(pb2.Table(schema="public", table="taxi"))
    print("Materialize", time.time() - t1)

    t1 = time.time()
    m = stub.GetTableInfo(pb2.Table(schema="public", table="taxi"))
    print("Info", time.time() - t1, m.rows)

    t1 = time.time()
    def query_gen(n):
        for i in range(n): yield pb2.Sql(sql="SELECT COUNT(*) as cnt, SUM(total_amount) as total_amount FROM taxi;", qid=i)        
    ipcs = stub.SelectIpc(query_gen(5))
    for ipc in ipcs:
        print(ipc.qid, pl.read_ipc(ipc.data))
    print("Async queries", time.time() - t1)