"""
Iceberg Schema Init Job — run ONCE on initial deploy.

Owns ALL Iceberg DDL. Creates all four tables if they do not exist:
  - iceberg.fraud.customers         — customer enrichment reference
  - iceberg.fraud.transactions_lake — full audit trail (partitioned by event_time)
  - iceberg.fraud.processed_batches — streaming batch dedup guard
  - iceberg.fraud.risk_profiles     — risk profile snapshot, synced from StarRocks by Airflow

Run before any other Spark job. Safe to re-run (CREATE TABLE IF NOT EXISTS).
"""
import os
from pyspark.sql import SparkSession

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
ICEBERG_DB_PASS  = os.environ["ICEBERG_DB_PASSWORD"]

spark = (
    SparkSession.builder
    .appName("init-iceberg-schema")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)
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
        reasons        STRING
    )
    USING iceberg
    PARTITIONED BY (days(event_time))
""")
print("✅ iceberg.fraud.transactions_lake ready")

# ── PROCESSED BATCHES — streaming batch dedup guard ───────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.fraud.processed_batches (
        batch_id     BIGINT NOT NULL,
        processed_at TIMESTAMP
    )
    USING iceberg
""")
print("✅ iceberg.fraud.processed_batches ready")

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
