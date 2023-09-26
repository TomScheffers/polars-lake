import grpc, concurrent, time
import proto.db_pb2 as pb2
import proto.db_pb2_grpc as pb2_grpc
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import io, socket, time
import numpy as np

# python -m grpc_tools.protoc --proto_path=. ./proto/db.proto --python_out=. --grpc_python_out=.

options = [('grpc.max_send_message_length', 1e10), ('grpc.max_receive_message_length', 1e10)]

# Load data
t = pl.read_parquet("data/stock_current/org_key=1/file.parquet").with_columns(pl.lit(1).alias('org_key'))
print(t)

stores = t.groupby(['store_key']).agg(pl.count()).to_dict(as_series=False)["store_key"]

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

    t1 = time.time()
    ipcs = stub.SelectsIpc(pb2.Sqls(sqls=[pb2.Sql(sql="SELECT * FROM stock_current WHERE sku_key = 1341286;"), pb2.Sql(sql="SELECT * FROM stock_current WHERE sku_key = 1341286;")]))
    print("2 queries took", time.time() - t1)

    t1 = time.time()
    ipcs = stub.SelectsIpc(pb2.Sqls(sqls=[pb2.Sql(sql="SELECT * FROM stock_current WHERE store_key = 101;")] * 10))
    print("10 queries took", time.time() - t1)

    t1 = time.time()
    ipcs = stub.SelectsIpc(pb2.Sqls(sqls=[pb2.Sql(sql=f"SELECT * FROM stock_current WHERE store_key = {s};") for s in stores]))
    print("All stores queries took", time.time() - t1)

def call(store_key):
    t1 = time.time()
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = pb2_grpc.DbStub(channel)
        ipc = stub.SelectIpc(pb2.Sql(sql=f"SELECT * FROM stock_current WHERE store_key = {store_key};"))
    df = pl.read_ipc(ipc.data)
    return time.time() - t1

with concurrent.futures.ThreadPoolExecutor(10) as executor:
    futures = []
    for s in stores[:20]:
        futures.append(executor.submit(call, (s,)))
    for future in concurrent.futures.as_completed(futures):
        print(future.result())