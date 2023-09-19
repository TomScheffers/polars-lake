import pyarrow as pa
import pyarrow.parquet as pq
from time import sleep
import socket

# Set up the data exchange socket
sk = socket.socket()
sk.connect(("127.0.0.1", 12989))
sk.listen()

# Load data
t = pq.read_table("data/stock_current/org_key=1/file.parquet")

# Accept incoming connection and stream the data away
connection, address = sk.accept()
dummy_socket_file = connection.makefile("wb")
with pa.RecordBatchStreamWriter(dummy_socket_file, t.schema) as writer:
    writer.write(t)