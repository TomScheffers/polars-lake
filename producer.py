import pyarrow as pa
import pyarrow.parquet as pq
import io, socket, time
import numpy as np

# Load data
ts = pq.read_table("data/stock_current/org_key=1/file.parquet")
ts = ts.append_column("org_key", pa.array(np.ones(ts.num_rows, dtype=int)))

for i in range(5):
    idxs, = np.where(ts.column('sku_key').to_numpy() % 5 == i % 5)
    #idxs = np.random.choice(np.arange(ts.num_rows), size=10, replace=False)
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
    sk.connect(("127.0.0.1", 7879))
    sk.send(bytes) 
    sk.close()