"""
Iceberg Expire-Snapshots Job
Runs expire_snapshots(older_than=now-12h, retain_last=1) on transactions_lake
+ processed_batches. Hourly via Airflow SparkKubernetesOperator.
"""
import datetime
import logging
import os

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
ICEBERG_DB_PASS  = os.environ["ICEBERG_DB_PASSWORD"]

TABLES = [
    "iceberg.fraud.transactions_lake",
    "iceberg.fraud.processed_batches",
]

# 12h snapshot retention — explicit older_than. retain_last is only a floor;
# without older_than, expire_snapshots falls back to Iceberg's 5-day default
# and the recent snapshot window never collapses (S4 root cause).
RETAIN_HOURS = 12
OLDER_THAN = (
    datetime.datetime.now(datetime.timezone.utc)
    - datetime.timedelta(hours=RETAIN_HOURS)
).strftime("%Y-%m-%d %H:%M:%S")

spark = (
    SparkSession.builder
    .appName("iceberg-expire-snapshots")
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

try:
    for table in TABLES:
        spark.sql(f"""
            CALL iceberg.system.expire_snapshots(
                table => '{table}',
                older_than => TIMESTAMP '{OLDER_THAN}',
                retain_last => 1
            )
        """)
        log.info("%s snapshots expired (older_than %s, retain_last 1)",
                 table, OLDER_THAN)

    log.info("Iceberg expire complete")
finally:
    spark.stop()
