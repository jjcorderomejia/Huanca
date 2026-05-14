"""
Iceberg Remove-Orphan-Files Job
Runs remove_orphan_files on transactions_lake + processed_batches.
Daily via Airflow SparkKubernetesOperator. Removes files referenced by no
snapshot (failed commits, old append-mode era). Default older_than = now-3d
protects the active iceberg-writer's in-flight commit files.
"""
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
        # Default older_than = now - 3 days — do NOT lower it; that protects
        # the active iceberg-writer's in-flight commit files from deletion.
        spark.sql(f"""
            CALL iceberg.system.remove_orphan_files(
                table => '{table}'
            )
        """)
        log.info("%s orphan files removed", table)

    log.info("Iceberg remove-orphan-files complete")
finally:
    spark.stop()
