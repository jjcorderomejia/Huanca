"""
Iceberg Schema Init Job — idempotent, safe (and intended) to re-run.

Owns ALL Iceberg DDL and table lifecycle properties. Creates all four tables
if they do not exist, and re-asserts metadata-cleanup TBLPROPERTIES on
transactions_lake + processed_batches every run:
  - iceberg.fraud.customers         — customer enrichment reference
  - iceberg.fraud.transactions_lake — full audit trail (partitioned by event_time)
  - iceberg.fraud.processed_batches — streaming batch dedup guard
  - iceberg.fraud.risk_profiles     — risk profile snapshot, synced from StarRocks by Airflow

Run before any other Spark job. Re-running converges new AND existing tables
to the desired schema + properties (CREATE TABLE IF NOT EXISTS and ALTER TABLE
SET TBLPROPERTIES are both idempotent). This is the home of the S4
metadata-bloat fix — see ops/S4-iceberg-metadata-bloat.md.
"""
import os
from pyspark.sql import SparkSession

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
ICEBERG_DB_PASS  = os.environ["ICEBERG_DB_PASSWORD"]

spark = (
    SparkSession.builder
    .appName("init-iceberg-schema")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "jdbc")
    .config("spark.sql.catalog.iceberg.uri",
            "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog")
    .config("spark.sql.catalog.iceberg.jdbc.user", "postgres")
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint",
            "http://minio.bigdata.svc.cluster.local:9000")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── CREATE DATABASE IF NOT EXISTS ─────────────────────────────────────
spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.fraud")

# ── CUSTOMERS — customer enrichment reference ─────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.fraud.customers (
        user_id        STRING,
        plan           STRING,
        avg_amount_30d DOUBLE,
        credit_limit   DOUBLE
    )
    USING iceberg
""")
print("✅ iceberg.fraud.customers ready")

# ── TRANSACTIONS LAKE — full audit trail, partitioned by event_time ───
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.fraud.transactions_lake (
        transaction_id STRING,
        user_id        STRING,
        amount         DOUBLE,
        merchant_id    STRING,
        merchant_lat   DOUBLE,
        merchant_lon   DOUBLE,
        status         STRING,
        event_time     TIMESTAMP,
        ingest_time    TIMESTAMP,
        fraud_score    INT,
        is_flagged     BOOLEAN,
        reasons        STRING,
        k_topic        STRING,
        k_partition    LONG,
        k_offset       LONG
    )
    USING iceberg
    PARTITIONED BY (days(event_time))
""")
spark.sql("""
    ALTER TABLE iceberg.fraud.transactions_lake SET TBLPROPERTIES (
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max'       = '20'
    )
""")
print("✅ iceberg.fraud.transactions_lake ready (lifecycle props set)")

# ── PROCESSED BATCHES — streaming batch dedup guard ───────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.fraud.processed_batches (
        batch_id     BIGINT NOT NULL,
        processed_at TIMESTAMP
    )
    USING iceberg
""")
spark.sql("""
    ALTER TABLE iceberg.fraud.processed_batches SET TBLPROPERTIES (
        'write.metadata.delete-after-commit.enabled' = 'true',
        'write.metadata.previous-versions-max'       = '20'
    )
""")
print("✅ iceberg.fraud.processed_batches ready (lifecycle props set)")

# ── RISK PROFILES — snapshot synced from StarRocks by Airflow ─────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.fraud.risk_profiles (
        user_id           STRING,
        avg_amount_30d    DOUBLE,
        stddev_amount     DOUBLE,
        avg_velocity_1h   DOUBLE,
        last_merchant_lat DOUBLE,
        last_merchant_lon DOUBLE,
        updated_at        TIMESTAMP
    )
    USING iceberg
""")
print("✅ iceberg.fraud.risk_profiles ready")

spark.stop()
