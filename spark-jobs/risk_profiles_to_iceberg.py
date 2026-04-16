"""
Risk Profiles → Iceberg Sync Job

Reads fraud.risk_profiles from StarRocks (batch, standalone — no streaming concurrency)
and overwrites iceberg.fraud.risk_profiles on MinIO/S3A.

Runs daily via Airflow SparkKubernetesOperator after refresh_risk_profiles task.
Streaming job reads from Iceberg — avoids AbstractStarrocksRDD Thrift scanner on
streaming executors which causes SIGABRT (exit code 134).
"""
import os
from pyspark.sql import SparkSession

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
ICEBERG_DB_PASS  = os.environ["ICEBERG_DB_PASSWORD"]
SR_HOST          = os.environ.get("STARROCKS_FE_HOST", "starrocks-fe-svc")
SR_USER          = os.environ.get("STARROCKS_USER", "root")
SR_PASSWORD      = os.environ.get("STARROCKS_PASSWORD", "")
SR_DB            = os.environ.get("STARROCKS_DB", "fraud")

spark = (
    SparkSession.builder
    .appName("risk-profiles-to-iceberg")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

risk_profiles = (
    spark.read
    .format("starrocks")
    .option("starrocks.fenodes", f"{SR_HOST}:8030")
    .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{SR_HOST}:9030")
    .option("starrocks.table.identifier", f"{SR_DB}.risk_profiles")
    .option("starrocks.user", SR_USER)
    .option("starrocks.password", SR_PASSWORD)
    .load()
)

count = risk_profiles.count()
print(f"Read {count} risk profiles from StarRocks")

(
    risk_profiles.write
    .format("iceberg")
    .mode("overwrite")
    .saveAsTable("iceberg.fraud.risk_profiles")
)
print(f"✅ Wrote {count} risk profiles to iceberg.fraud.risk_profiles")

spark.stop()
