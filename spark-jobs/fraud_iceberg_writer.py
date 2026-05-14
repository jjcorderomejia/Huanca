"""
Iceberg Writer — stateless consumer of transactions-scored Kafka topic.
MERGEs scored events into iceberg.fraud.transactions_lake exactly-once
via Kafka offset composite key. 1-min trigger.
"""
import json
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, LongType, BooleanType, IntegerType,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── ENV ──────────────────────────────────────────────────────────────
BOOTSTRAP        = os.environ["REDPANDA_BOOTSTRAP"]
TOPIC_SCORED     = os.environ.get("TOPIC_SCORED", "transactions-scored")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")
CHECKPOINT_BASE  = os.environ.get("CHECKPOINT_LOCATION", "s3a://checkpoints/fraud-iceberg-writer-v1")
TRIGGER_INTERVAL = os.environ.get("SPARK_TRIGGER_INTERVAL", "1 minute")
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
ICEBERG_DB_PASS  = os.environ["ICEBERG_DB_PASSWORD"]

_KAFKA_SSL = {
    "kafka.security.protocol":                     "SSL",
    "kafka.ssl.truststore.location":               "/etc/redpanda-certs/ca.crt",
    "kafka.ssl.truststore.type":                   "PEM",
    "kafka.ssl.endpoint.identification.algorithm": "",
}

# ── SCHEMA — must match scorer's transactions-scored payload ──────────
scored_schema = StructType([
    StructField("transaction_id", StringType(),    True),
    StructField("user_id",        StringType(),    True),
    StructField("amount",         DoubleType(),    True),
    StructField("merchant_id",    StringType(),    True),
    StructField("merchant_lat",   DoubleType(),    True),
    StructField("merchant_lon",   DoubleType(),    True),
    StructField("status",         StringType(),    True),
    StructField("event_time",     TimestampType(), True),
    StructField("ingest_time",    TimestampType(), True),
    StructField("fraud_score",    IntegerType(),   True),
    StructField("is_flagged",     BooleanType(),   True),
    StructField("reasons",        StringType(),    True),
    StructField("k_topic",        StringType(),    True),
    StructField("k_partition",    LongType(),      True),
    StructField("k_offset",       LongType(),      True),
])

# ── SPARK SESSION ─────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("fraud-iceberg-writer")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "jdbc")
    .config("spark.sql.catalog.iceberg.uri",
            "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog")
    .config("spark.sql.catalog.iceberg.jdbc.user", "postgres")
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.bigdata.svc.cluster.local:9000")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── READ FROM transactions-scored ─────────────────────────────────────
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC_SCORED)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    .options(**_KAFKA_SSL)
    .load()
)

scored = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .withColumn("row", from_json(col("json"), scored_schema))
       .select("row.*")
)

# ── MERGE INTO transactions_lake (exactly-once by Kafka offsets) ──────
def write_iceberg(batch_df, batch_id: int):
    if batch_df.isEmpty():
        return
    batch_df.createOrReplaceTempView("_batch")
    try:
        spark.sql("""
            MERGE INTO iceberg.fraud.transactions_lake AS t
            USING _batch AS s
            ON  t.k_topic     = s.k_topic
            AND t.k_partition = s.k_partition
            AND t.k_offset    = s.k_offset
            WHEN NOT MATCHED THEN INSERT *
        """)
        log.info(json.dumps({"event": "merge_ok", "batch_id": batch_id, "rows": batch_df.count()}))
    except Exception as e:
        logging.error(json.dumps({"event": "merge_failed", "batch_id": batch_id, "error": str(e)}))

writer_query = (
    scored.writeStream
    .foreachBatch(write_iceberg)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/main")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

spark.streams.awaitAnyTermination()
