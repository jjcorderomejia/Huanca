"""
Iceberg Remove-Orphan-Files Job
Runs remove_orphan_files on transactions_lake + processed_batches.
Daily via Airflow SparkKubernetesOperator. Removes files referenced by no
snapshot (failed commits, old append-mode era). Explicit older_than = now-5d
protects the active iceberg-writer's in-flight commit files.
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

# 5-day older_than — explicit (was relying on Iceberg's implicit 3-day default).
# Protects the active iceberg-writer's in-flight commit files; conservative on
# purpose — too-long only delays cleanup, too-short risks deleting live files.
OLDER_THAN_DAYS = 5
OLDER_THAN = (
    datetime.datetime.now(datetime.timezone.utc)
    - datetime.timedelta(days=OLDER_THAN_DAYS)
).strftime("%Y-%m-%d %H:%M:%S")

spark = (
    SparkSession.builder
    .appName("iceberg-remove-orphan-files")
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
        # older_than = now - 5 days, explicit (Iceberg's implicit default is
        # 3d) — protects the active iceberg-writer's in-flight commit files.
        spark.sql(f"""
            CALL iceberg.system.remove_orphan_files(
                table => '{table}',
                older_than => TIMESTAMP '{OLDER_THAN}'
            )
        """)
        log.info("%s orphan files removed (older_than %s)", table, OLDER_THAN)

    log.info("Iceberg remove-orphan-files complete")
finally:
    spark.stop()
