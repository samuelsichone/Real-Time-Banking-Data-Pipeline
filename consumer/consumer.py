import json
import io
from datetime import datetime
from kafka import KafkaConsumer
from minio import Minio
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# -------------------------------------------------
# Config
# -------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = ["localhost:29092"]
TOPIC_PATTERN = "banking.public.*"

MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "banking-parquet"

BATCH_SIZE = 50   #  batch size



# -------------------------------------------------
# Kafka Consumer
# -------------------------------------------------
print("⏳ Creating Kafka consumer...")

consumer = KafkaConsumer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=None,
    consumer_timeout_ms=60000,
    api_version=(3, 4, 0),
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

consumer.subscribe(pattern=TOPIC_PATTERN)

print("✅ Kafka consumer created")
print(f"📡 Subscribed to topics: {TOPIC_PATTERN}")

# -------------------------------------------------
# MinIO Client
# -------------------------------------------------
print("⏳ Connecting to MinIO...")

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

if not minio_client.bucket_exists(MINIO_BUCKET):
    minio_client.make_bucket(MINIO_BUCKET)
    print(f"🪣 Created bucket: {MINIO_BUCKET}")
else:
    print(f"🪣 Using bucket: {MINIO_BUCKET}")

# -------------------------------------------------
# Buffers (per table)
# -------------------------------------------------
buffers = {}

# -------------------------------------------------
# Helper: Write Parquet to MinIO
# -------------------------------------------------
def flush_to_minio(table_name, rows):
    if not rows:
        return

    df = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df)

    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)

    timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H")
    object_name = (
        f"{table_name}/"
        f"{timestamp}/"
        f"batch-{int(datetime.utcnow().timestamp())}.parquet"
    )

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    print(f"✅ Parquet written → {object_name}")

# -------------------------------------------------
# Consume Kafka → Parquet → MinIO
# -------------------------------------------------
try:
    for msg in consumer:
        event = msg.value

        table_name = event.get("source", {}).get("table", "unknown_table")

        # Initialize buffer per table
        if table_name not in buffers:
            buffers[table_name] = []

        # Ignore deletes (or handle separately)
        if event.get("after") is None:
            continue

        row = event["after"]
        row["_op"] = event.get("op")
        row["_ts_ms"] = event.get("ts_ms")

        buffers[table_name].append(row)

        print(
            f"📥 Buffered {table_name}: "
            f"{len(buffers[table_name])}/{BATCH_SIZE}"
        )

        if len(buffers[table_name]) >= BATCH_SIZE:
            flush_to_minio(table_name, buffers[table_name])
            buffers[table_name].clear()

finally:
    # Flush remaining data on shutdown
    for table_name, rows in buffers.items():
        flush_to_minio(table_name, rows)

    consumer.close()
    print("⚠️ Consumer stopped")
