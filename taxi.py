import grpc, concurrent, time, os
import proto.db_pb2 as pb2
import proto.db_pb2_grpc as pb2_grpc
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import io, socket, time
import numpy as np

# python -m grpc_tools.protoc --proto_path=. ./proto/db.proto --python_out=. --grpc_python_out=.

def table_to_ipc(frame):
    b = io.BytesIO()
    frame.write_ipc(b)
    b.seek(0)
    return b.read()

schema  = pl.scan_parquet("data/taxi/yellow_tripdata_2023-01.parquet").with_columns(pl.col("tpep_pickup_datetime").dt.date().alias("date")).schema

# Create table
with grpc.insecure_channel("localhost:50051") as channel:
    stub = pb2_grpc.DbStub(channel)

    def root_to_generator(root, schema, size=25_000, begin=0, end=-1):
        # Load data
        for p in os.listdir(root)[begin:end]:
            print(p)
            frame = pl.read_parquet(root + p).with_columns(pl.col("tpep_pickup_datetime").dt.date().alias("date"))
            if "Airport_fee" in frame.columns: frame = frame.drop("Airport_fee")
            if "airport_fee" in frame.columns: frame = frame.drop("airport_fee")
            frame = frame.cast({k:v for k,v in schema.items() if k in frame.columns}, strict=False)
            for t in frame.iter_slices(size):
                yield pb2.SourceIpc(schema="public", table="taxi", data=table_to_ipc(t), partitions=[], buckets=[])

    t1 = time.time()
    m = stub.CreateTableStream(root_to_generator("data/taxi/", schema=schema, size=25_000, begin=0, end=1))
    print("Create (Stream)", time.time() - t1)

    t1 = time.time()
    m = stub.InsertTableStream(root_to_generator("data/taxi/", schema=schema, size=25_000, begin=1, end=None))
    print("Insert (Stream)", time.time() - t1)

    t1 = time.time()
    m = stub.MaterializeTable(pb2.Table(schema="public", table="taxi"))
    print("Materialize", time.time() - t1)

    t1 = time.time()
    m = stub.GetTableInfo(pb2.Table(schema="public", table="taxi"))
    print("Info", time.time() - t1, m.rows)

    t1 = time.time()
    ipcs = stub.SelectsIpc(pb2.Sqls(sqls=[pb2.Sql(sql="SELECT COUNT(*) as cnt, SUM(total_amount) as total_amount FROM taxi;")]))
    for ipc in ipcs.results:
        print(pl.read_ipc(ipc.data))
    print("Single date took", time.time() - t1)
