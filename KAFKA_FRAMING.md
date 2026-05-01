# `.kfb.zst` framing

Topics whose payloads are not self-delimited JSON (e.g. the OpenINTEL Zonestream
`*_measurements` topics, which use Confluent-framed Avro: `0x00 || u32-BE
schema_id || avro_payload`) are stored without decoding so that no information
is lost. Each Kafka message becomes one frame:

```
+--------------------+--------------------+----------------+
| u64 BE timestamp_ms| u32 BE payload_len | payload bytes  |
+--------------------+--------------------+----------------+
   8 bytes              4 bytes              payload_len bytes
```

Frames are concatenated and the entire hour's stream is then zstd-compressed
into `data/<topic>/YYYY/MM/DD/HH.kfb.zst`.

The 12-byte header is a small price for being able to:

- recover the broker timestamp (which is otherwise discarded once the file is
  written); and
- replay each message back through a future Avro / schema-registry decoder
  exactly as it came off the wire.

## Reading a `.kfb.zst` file

```python
import struct, zstandard as zstd

HDR = struct.Struct(">QI")  # u64 timestamp_ms, u32 payload_len

def iter_frames(path):
    dctx = zstd.ZstdDecompressor()
    with open(path, "rb") as f, dctx.stream_reader(f) as r:
        while True:
            head = r.read(HDR.size)
            if not head:
                return
            if len(head) < HDR.size:
                raise ValueError("truncated frame header")
            ts_ms, ln = HDR.unpack(head)
            payload = r.read(ln)
            if len(payload) != ln:
                raise ValueError("truncated frame payload")
            yield ts_ms, payload
```

To decode the Confluent-Avro payload once a schema is in hand:

```python
# payload[0] is 0x00 (magic), payload[1:5] is u32-BE schema id
import struct
magic, schema_id = payload[0], struct.unpack(">I", payload[1:5])[0]
avro_bytes = payload[5:]
# decode `avro_bytes` with the schema registered under id=schema_id
```
