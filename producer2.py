import pyarrow as pa
import pyarrow.parquet as pq
import io, socket
import numpy as np

# Load data
ts = pq.read_table("data/stock_current/org_key=1/file.parquet")

for i in range(5):
    idxs, = np.where(ts.column('sku_key').to_numpy() % 5 == i)
    # idxs = np.random.choice(np.arange(ts.num_rows), size=10000, replace=False)
    t = ts.take(idxs)
    print(t.shape)

    # Schema
    schema = t.schema
    schema = schema.with_metadata({"table": "stock_current"})

    # Accept incoming connection and stream the data away
    file = io.BytesIO()

    with pa.RecordBatchStreamWriter(file, schema) as writer:
        for batch in t.to_batches():
            writer.write_batch(batch)
    writer.close()

    # File to bytes
    file.seek(0)
    bytes = file.read()
    print(len(bytes))

    # Set up the data exchange socket
    sk = socket.socket()
    sk.connect(("127.0.0.1", 7878))
    sk.send(bytes)
    sk.close()
