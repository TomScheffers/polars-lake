import pyarrow as pa
import pyarrow.parquet as pq
from time import sleep
import io, socket

# Set up the data exchange socket
sk = socket.socket()
sk.connect(("127.0.0.1", 7878))

# Load data
t = pq.read_table("data/stock_current/org_key=1/file.parquet")
schema = t.schema
schema = schema.with_metadata({"table": "stock_current"})

# Accept incoming connection and stream the data away
file = io.BytesIO()

with pa.RecordBatchStreamWriter(file, schema) as writer:
    for batch in t.to_batches():
        writer.write_batch(batch)
writer.close()

# Send bytes to url
file.seek(0)
bytes = file.read()
print(len(bytes))
sk.send(bytes)
sk.close()
