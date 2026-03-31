"""
Customer CSV → Iceberg Data Load Job

Reads customer enrichment CSV from ConfigMap mount and overwrites
iceberg.fraud.customers (pure data load — DDL owned by init_iceberg_schema.py).

Execution:
  - Initial deploy: K8s SparkApplication (customer-csv-to-iceberg.yaml.tpl)
  - Daily refresh: Airflow DAG (pending implementation)
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

CUSTOMER_CSV_PATH = os.environ.get("CUSTOMER_CSV_PATH", "/opt/enrichment/customer.csv")
MINIO_ACCESS_KEY  = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY  = os.environ["MINIO_SECRET_KEY"]
ICEBERG_DB_PASS   = os.environ["ICEBERG_DB_PASSWORD"]

spark = (
    SparkSession.builder
    .appName("customer-csv-to-iceberg")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── READ CUSTOMER CSV ─────────────────────────────────────────────────
customers = (
    spark.read
    .option("header", "true")
    .csv(CUSTOMER_CSV_PATH)
    .select(
        col("user_id"),
        col("plan"),
        col("avg_amount_30d").cast("double"),
        col("credit_limit").cast("double")
    )
)

# ── WRITE TO ICEBERG — overwrite (pure data load, DDL in init_iceberg_schema.py) ──
customers.writeTo("iceberg.fraud.customers").overwritePartitions()

print(f"✅ {customers.count()} customer records written to iceberg.fraud.customers")

spark.stop()
