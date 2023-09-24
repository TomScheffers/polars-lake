import grpc, concurrent, time
import proto.db_pb2 as pb2
import proto.db_pb2_grpc as pb2_grpc
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import io, socket, time
import numpy as np

options = [('grpc.max_send_message_length', 1e10), ('grpc.max_receive_message_length', 1e10)]

# Load data
t = pl.read_parquet("data/stock_current/org_key=1/file.parquet").with_columns(pl.lit(1).alias('org_key'))
print(t)

# Create table
with grpc.insecure_channel("localhost:50051", options=options) as channel:
    stub = pb2_grpc.DbStub(channel)

    for idx, frame in enumerate(t.iter_slices(80_000)):
        b = io.BytesIO()
        frame.write_ipc(b)
        b.seek(0)
        if idx == 0:
            m = stub.CreateTable(pb2.SourceIpc(schema="public", table="stock_current", data=b.read(), partitions="org_key", buckets="sku_key"))
        else:
            m = stub.InsertTable(pb2.SourceIpc(schema="public", table="stock_current", data=b.read(), partitions="org_key", buckets="sku_key"))
    m = stub.MaterializeTable(pb2.Table(schema="public", table="stock_current"))

def call():
    t1 = time.time()
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = pb2_grpc.DbStub(channel)
        ipc = stub.SelectIpc(pb2.Sql(sql="SELECT * FROM stock_current WHERE store_key = 101;"))
    df = pl.read_ipc(ipc.data)
    return time.time() - t1

with concurrent.futures.ThreadPoolExecutor(30) as executor:
    futures = []
    for _ in range(10):
        futures.append(executor.submit(call))
    for future in concurrent.futures.as_completed(futures):
        print(future.result())