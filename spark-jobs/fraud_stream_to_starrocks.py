"""
Real-Time Payment Fraud Detection
Redpanda → Spark Structured Streaming → StarRocks + Iceberg + DLQ

Architecture:
  - At-least-once delivery (Kafka semantics)
  - Idempotent sink (StarRocks Primary Key upserts)
  - Stateful velocity via flatMapGroupsWithState + ProcessingTimeTimeout (no watermark dependency)
  - Rule-based fraud scoring (velocity · zscore · geo)
  - DLQ for malformed records
  - Iceberg append for full audit trail (JDBC catalog — safe concurrent writes)
  - Customer enrichment from iceberg.fraud.customers (daily Airflow refresh, TTL-cached)
  - Batch dedup via iceberg.fraud.processed_batches — safe accumulator on retry
"""
import json
import logging
import os
import time
from math import radians, sin, cos, sqrt, atan2

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, current_timestamp, when,
    to_timestamp, expr, struct, to_json, udf,
    count, sum as spark_sum, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, LongType, ArrayType
)
from pyspark.sql.streaming.state import GroupStateTimeout

# ── ENV ──────────────────────────────────────────────────────────────
BOOTSTRAP               = os.environ["REDPANDA_BOOTSTRAP"]
TOPIC_IN                = os.environ.get("TOPIC_IN",                "transactions-raw")
TOPIC_DLQ               = os.environ.get("TOPIC_DLQ",               "transactions-dlq")
TOPIC_CLEAN             = os.environ.get("TOPIC_CLEAN",             "transactions-clean")
TOPIC_ALERTS            = os.environ.get("TOPIC_ALERTS",            "fraud-alerts")
SR_HOST                 = os.environ.get("STARROCKS_HOST",           "starrocks-fe-svc.bigdata.svc.cluster.local")
SR_PORT                 = os.environ.get("STARROCKS_PORT",           "9030")
SR_DB                   = os.environ.get("STARROCKS_DB",             "fraud")
SR_USER                 = os.environ.get("STARROCKS_USER",           "root")
SR_PASSWORD             = os.environ["STARROCKS_PASSWORD"]           # required — no default, fails fast if missing
MINIO_ACCESS_KEY        = os.environ["MINIO_ACCESS_KEY"]             # required — injected from K8s Secret
MINIO_SECRET_KEY        = os.environ["MINIO_SECRET_KEY"]             # required — injected from K8s Secret
ICEBERG_DB_PASS         = os.environ["ICEBERG_DB_PASSWORD"]          # required — Postgres password for Iceberg JDBC catalog
CUSTOMER_ICEBERG_TABLE  = os.environ.get("CUSTOMER_ICEBERG_TABLE",   "iceberg.fraud.customers")
CUSTOMER_CACHE_TTL      = int(os.environ.get("CUSTOMER_CACHE_TTL_SEC", "3600"))  # 1 hour — aligns with daily Airflow refresh
STARTING_OFFSETS        = os.environ.get("STARTING_OFFSETS",         "earliest")  # safe default — no gap on checkpoint loss
CHECKPOINT_BASE         = os.environ.get("CHECKPOINT_LOCATION",      "s3a://checkpoints/fraud-stream")
TRIGGER_INTERVAL        = os.environ.get("SPARK_TRIGGER_INTERVAL",   "10 seconds")
COLD_TRIGGER_INTERVAL   = os.environ.get("SPARK_COLD_TRIGGER_INTERVAL", "5 minutes")
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))  # cap batch size — prevent OOM on backlog

# ── FRAUD THRESHOLDS (overridable via env) ────────────────────────────
VELOCITY_THRESHOLD  = int(os.environ.get("FRAUD_VELOCITY_THRESHOLD",  "5"))
ZSCORE_THRESHOLD    = float(os.environ.get("FRAUD_ZSCORE_THRESHOLD",  "3.0"))
GEO_SPEED_THRESHOLD = float(os.environ.get("FRAUD_GEO_KMH_THRESHOLD", "500.0"))
FRAUD_SCORE_CUTOFF  = int(os.environ.get("FRAUD_SCORE_CUTOFF",        "60"))

# ── KAFKA SSL — Redpanda uses TLS on all listeners ─────────────────────
# CA cert mounted from secret fraud-redpanda-default-root-certificate.
_KAFKA_SSL: dict = {
    "kafka.security.protocol":                     "SSL",
    "kafka.ssl.truststore.location":               "/etc/redpanda-certs/ca.crt",
    "kafka.ssl.truststore.type":                   "PEM",
    "kafka.ssl.endpoint.identification.algorithm": "",
}

# ── SCHEMA ────────────────────────────────────────────────────────────
txn_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id",        StringType(), True),
    StructField("amount",         DoubleType(), True),
    StructField("merchant_id",    StringType(), True),
    StructField("merchant_lat",   DoubleType(), True),
    StructField("merchant_lon",   DoubleType(), True),
    StructField("status",         StringType(), True),
    StructField("timestamp",      StringType(), True),
])

# ── VELOCITY STATE ────────────────────────────────────────────────────
# Rolling 5-min transaction count per user maintained in Spark state store.
# ProcessingTimeTimeout evicts idle users after VELOCITY_STATE_TIMEOUT —
# ensures state is bounded even when a user stops transacting entirely.
VELOCITY_WINDOW_SEC    = 5 * 60   # 5-minute rolling window
VELOCITY_STATE_TIMEOUT = 10 * 60 * 1000  # 10-minute idle eviction in ms — wall-clock, not event-time

_VELOCITY_OUTPUT_SCHEMA = StructType([
    StructField("transaction_id", StringType(),    True),
    StructField("user_id",        StringType(),    True),
    StructField("amount",         DoubleType(),    True),
    StructField("merchant_id",    StringType(),    True),
    StructField("merchant_lat",   DoubleType(),    True),
    StructField("merchant_lon",   DoubleType(),    True),
    StructField("status",         StringType(),    True),
    StructField("event_time",     TimestampType(), True),
    StructField("ingest_time",    TimestampType(), True),
    StructField("velocity_5min",  LongType(),      True),
    StructField("raw_json",       StringType(),    True),
    StructField("k_topic",        StringType(),    True),
    StructField("k_partition",    LongType(),      True),
    StructField("k_offset",       LongType(),      True),
])

# State schema: single array field holding event-time epoch floats.
# Tuple access: state.get[0] → list[float]
_VELOCITY_STATE_SCHEMA = StructType([
    StructField("timestamps", ArrayType(DoubleType()), True),
])

def _velocity_state_fn(key, pdf_iter, state):
    """
    applyInPandasWithState function — rolling 5-min velocity per user.

    State: array of event_time epochs (float, seconds since epoch).
    Window cutoff uses max(event_time) in the batch — pure event-time
    basis, tolerant of ingestion lag (no processing-time / event-time
    mismatch that would deflate velocity on delayed data).

    On ProcessingTimeTimeout (user idle > VELOCITY_STATE_TIMEOUT):
    remove state — prevents unbounded state growth for inactive users.
    """
    import pandas as pd
    import time

    if state.hasTimedOut:
        state.remove()
        return

    # state.get is a tuple; index 0 is the timestamps array field
    prev_ts    = list(state.get[0]) if state.exists else []
    batch_pdfs = list(pdf_iter)  # consume iterator once — reused below

    # Collect event-time epochs from this micro-batch
    new_ts = []
    for pdf in batch_pdfs:
        for ts in pdf["event_time"].dropna():
            new_ts.append(pd.Timestamp(ts).timestamp())

    # Pure event-time cutoff: anchor to latest event seen, not wall clock.
    # Falls back to time.time() only on a timeout batch (new_ts is empty).
    all_seen = prev_ts + new_ts
    now      = max(all_seen) if all_seen else time.time()
    cutoff   = now - VELOCITY_WINDOW_SEC

    all_ts   = [t for t in all_seen if t >= cutoff]
    velocity = len(all_ts)

    state.update((all_ts,))
    state.setTimeoutDuration(VELOCITY_STATE_TIMEOUT)

    _out_cols = [
        "transaction_id", "user_id", "amount", "merchant_id",
        "merchant_lat", "merchant_lon", "status",
        "event_time", "ingest_time",
        "raw_json", "k_topic", "k_partition", "k_offset",
    ]
    for pdf in batch_pdfs:
        out = pdf[_out_cols].copy()
        out["velocity_5min"] = velocity
        yield out

# ── HAVERSINE UDF (geo distance in km) ───────────────────────────────
# Used by compute_geo inside write_hot_sinks / write_cold_sinks foreachBatch
def haversine(lat1, lon1, lat2, lon2):
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return 0.0
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

# ── COMPUTE GEO SPEED UDF ─────────────────────────────────────────────
# Looks up last known location from risk_profiles broadcast cache
# Returns speed km/h between last and current merchant location
def compute_geo(user_id, curr_lat, curr_lon, event_time):
    profile = risk_bc.value.get(user_id)
    if not profile or profile.get("last_merchant_lat") is None:
        return 0.0
    dist = haversine(
        profile["last_merchant_lat"], profile["last_merchant_lon"],
        curr_lat, curr_lon
    )
    if profile.get("updated_at") and event_time:
        delta_hours = (event_time.timestamp() - profile["updated_at"].timestamp()) / 3600
        return dist / delta_hours if delta_hours > 0 else 0.0
    return 0.0

compute_geo_udf = udf(compute_geo, DoubleType())

# ── SPARK SESSION ─────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("fraud-stream-to-starrocks")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "jdbc")
    .config("spark.sql.catalog.iceberg.uri",
            "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog")
    .config("spark.sql.catalog.iceberg.jdbc.user", "postgres")
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)  # Iceberg JDBC catalog password from K8s Secret
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint",
            "http://minio.bigdata.svc.cluster.local:9000")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)  # MinIO credentials from K8s Secret
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── RISK PROFILE BROADCAST CACHE ──────────────────────────────────────
# TTL-cached inside foreachBatch — refreshed sequentially before each micro-batch.
# Sequential execution eliminates concurrent Spark job submission that caused
# executor SIGABRT (exit code 134) with the former background-thread pattern.
# On cold start cache is empty; geo score defaults to 0 (no false positives).
RISK_CACHE_TTL = int(os.environ.get("RISK_CACHE_TTL_SEC", "60"))
_risk_cache: dict = {"loaded_at": 0.0}

risk_bc = spark.sparkContext.broadcast({})

def refresh_risk_if_stale():
    global risk_bc
    if (time.time() - _risk_cache["loaded_at"]) < RISK_CACHE_TTL:
        return
    try:
        data = (
            spark.read
            .format("iceberg")
            .load("iceberg.fraud.risk_profiles")
            .rdd.map(lambda r: (r.user_id, r.asDict()))
            .collectAsMap()
        )
        risk_bc = spark.sparkContext.broadcast(data)
        _risk_cache["loaded_at"] = time.time()
    except Exception as e:
        logging.error(json.dumps({"event": "risk_refresh_failed", "error": str(e)}))

# ── CUSTOMER ENRICHMENT CACHE ──────────────────────────────────────────
# TTL-based in-memory cache. Loaded from iceberg.fraud.customers on MinIO.
# Populated daily by Airflow (customer_csv_to_iceberg.py).
# Refreshes every CUSTOMER_CACHE_TTL seconds inside foreachBatch.
_customer_cache: dict = {"df": None, "loaded_at": 0.0}

def get_customers_df():
    """Returns customer enrichment DataFrame from Iceberg, refreshing if TTL expired.
    Falls back to empty schema DataFrame if table not yet populated (first-boot / Airflow
    hasn't run yet). Stream degrades gracefully — plan and avg_amount_30d will be null
    until the daily fraud_customer_refresh DAG seeds the data."""
    now = time.time()
    if _customer_cache["df"] is None or (now - _customer_cache["loaded_at"]) > CUSTOMER_CACHE_TTL:
        try:
            df = (
                spark.read
                .format("iceberg")
                .load(CUSTOMER_ICEBERG_TABLE)
                .select(
                    col("user_id").alias("c_user_id"),
                    col("plan"),
                    col("avg_amount_30d").cast("double")
                )
            )
            _ = df.schema  # force JVM analysis — raises AnalysisException here if table missing
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "table or view" in str(e).lower():
                logging.warning(json.dumps({
                    "event": "customer_table_not_found",
                    "table": CUSTOMER_ICEBERG_TABLE,
                    "note": "using empty enrichment until Airflow seeds data"
                }))
                df = spark.createDataFrame(
                    [], "c_user_id STRING, plan STRING, avg_amount_30d DOUBLE"
                )
            else:
                raise
        _customer_cache["df"] = df
        _customer_cache["loaded_at"] = now
    return _customer_cache["df"]

# ── OBSERVABILITY ACCUMULATORS ─────────────────────────────────────────
# Driver-side counters visible in Spark UI.
# Guarded by iceberg.fraud.processed_batches — incremented only once per batch_id.
acc_total    = spark.sparkContext.accumulator(0)
acc_flagged  = spark.sparkContext.accumulator(0)
acc_geo_hits = spark.sparkContext.accumulator(0)

# ── READ FROM REDPANDA ─────────────────────────────────────────────────
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)  # cap batch size — prevent OOM on backlog
    .options(**_KAFKA_SSL)
    .load()
)

raw_with_meta = raw.select(
    col("topic").cast("string").alias("k_topic"),
    col("partition").cast("int").alias("k_partition"),
    col("offset").cast("long").alias("k_offset"),
    col("value").cast("string").alias("raw_json"),
)

parsed = raw_with_meta.withColumn("j", from_json(col("raw_json"), txn_schema))

# ── VALIDATION ────────────────────────────────────────────────────────
expanded = (
    parsed
    .withColumn("transaction_id", col("j.transaction_id"))
    .withColumn("user_id",        col("j.user_id"))
    .withColumn("amount",         col("j.amount"))
    .withColumn("merchant_id",    col("j.merchant_id"))
    .withColumn("merchant_lat",   col("j.merchant_lat"))
    .withColumn("merchant_lon",   col("j.merchant_lon"))
    .withColumn("status",         col("j.status"))
    .withColumn("event_time",     to_timestamp(col("j.timestamp")))
    .withColumn("ingest_time",    current_timestamp())
)

dlq_reason = (
    when(col("j").isNull(),                                                lit("schema_parse_error"))
    .when(col("transaction_id").isNull() | (col("transaction_id") == ""), lit("missing_transaction_id"))
    .when(col("user_id").isNull()        | (col("user_id") == ""),        lit("missing_user_id"))
    .when(col("amount").isNull(),                                          lit("missing_amount"))
    .when(col("amount") <= 0,                                             lit("non_positive_amount"))
    .when(col("merchant_id").isNull()    | (col("merchant_id") == ""),    lit("missing_merchant_id"))
    .when(col("event_time").isNull(),                                      lit("bad_timestamp"))
    .otherwise(lit(None))
)

tagged = expanded.withColumn("dlq_reason", dlq_reason)
bad    = tagged.filter(col("dlq_reason").isNotNull())
good   = tagged.filter(col("dlq_reason").isNull()).drop("dlq_reason", "j")

# ── FEATURE ENGINEERING ───────────────────────────────────────────────
# Velocity: rolling 5-min count per user via applyInPandasWithState.
# ProcessingTimeTimeout evicts idle user state on wall-clock inactivity.
# Window cutoff anchored to max(event_time) — event-time correct, lag tolerant.
# Emits one enriched row per input record immediately each micro-batch.
# No watermark cascade, no stream-stream join buffering.
final_df = (
    good
    .groupBy("user_id")
    .applyInPandasWithState(
        func=_velocity_state_fn,
        outputStructType=_VELOCITY_OUTPUT_SCHEMA,
        stateStructType=_VELOCITY_STATE_SCHEMA,
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )
)

# ── HOT SINKS — StarRocks + Kafka (10s trigger) ─────────────────────
def write_hot_sinks(batch_df, batch_id: int):
    global acc_total, acc_flagged, acc_geo_hits
    if batch_df.isEmpty():
        return

    refresh_risk_if_stale()

    # ── VELOCITY SCORE ────────────────────────────────────────────
    # velocity_5min arrives from _velocity_state_fn (flatMapGroupsWithState).
    # Score computed here to keep the streaming plan free of Column expressions.
    batch_df = batch_df.withColumn("score_velocity",
        when(col("velocity_5min") > VELOCITY_THRESHOLD, lit(40)).otherwise(lit(0)))

    # ── CUSTOMER ENRICHMENT ───────────────────────────────────────
    # TTL-cached Iceberg read — reloads at most every CUSTOMER_CACHE_TTL seconds.
    # Populated daily by Airflow (customer_csv_to_iceberg.py).
    customers_df = get_customers_df()
    batch_df = (
        batch_df
        .join(broadcast(customers_df), batch_df.user_id == customers_df.c_user_id, "left")
        .drop("c_user_id")
        .withColumn("plan",
            when(col("plan").isNull(), lit("unknown")).otherwise(col("plan")))
        .withColumn("avg_amount_30d",
            when(col("avg_amount_30d").isNull(), lit(0.0)).otherwise(col("avg_amount_30d")))
    )

    # ── ZSCORE ────────────────────────────────────────────────────
    # Simplified stddev = 30% of mean — known limitation, pending business calibration
    batch_df = (
        batch_df
        .withColumn("amount_zscore",
            when(col("avg_amount_30d") > 0,
                (col("amount") - col("avg_amount_30d")) / (col("avg_amount_30d") * 0.3))
            .otherwise(lit(0.0)))
        .withColumn("score_zscore",
            when(col("amount_zscore") > ZSCORE_THRESHOLD, lit(30)).otherwise(lit(0)))
    )

    # ── GEO ENRICHMENT ────────────────────────────────────────────
    # Computes geo speed km/h using risk_profiles broadcast cache
    batch_df = (
        batch_df
        .withColumn("geo_speed_kmh",
            compute_geo_udf(
                col("user_id"), col("merchant_lat"),
                col("merchant_lon"), col("event_time")
            )
        )
        .withColumn("score_geo",
            when(col("geo_speed_kmh") > GEO_SPEED_THRESHOLD, lit(30)).otherwise(lit(0)))
    )

    # ── FINAL SCORE ───────────────────────────────────────────────
    batch_df = (
        batch_df
        .withColumn("fraud_score",
            col("score_velocity") + col("score_zscore") + col("score_geo"))
        .withColumn("is_flagged", col("fraud_score") >= FRAUD_SCORE_CUTOFF)
        .withColumn("reasons", expr("""
            concat_ws(',',
              case when score_velocity > 0 then 'HIGH_VELOCITY' end,
              case when score_zscore   > 0 then 'AMOUNT_ANOMALY' end,
              case when score_geo      > 0 then 'GEO_IMPOSSIBLE' end
            )
        """))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # ── OBSERVABILITY — single aggregation pass ────────────────────
    stats = batch_df.agg(
        count("*").alias("total"),
        spark_sum(when(col("is_flagged"), lit(1)).otherwise(lit(0))).alias("flagged"),
        spark_sum(when(col("score_geo") > 0, lit(1)).otherwise(lit(0))).alias("geo_hits")
    ).collect()[0]

    total    = stats["total"]
    flagged  = stats["flagged"] or 0
    geo_hits = stats["geo_hits"] or 0

    # ── BATCH DEDUP — guard accumulators against retry double-count ─
    # iceberg.fraud.processed_batches is source of truth for seen batch_ids
    try:
        already_processed = (
            spark.read.format("iceberg").load("iceberg.fraud.processed_batches")
            .filter(col("batch_id") == batch_id)
            .count() > 0
        )
    except Exception:
        already_processed = False  # table doesn't exist on first run

    if not already_processed:
        acc_total    += total
        acc_flagged  += flagged
        acc_geo_hits += geo_hits

    try:
        # ── STARROCKS — transactions ──────────────────────────────
        try:
            (
                batch_df.select(
                    "transaction_id", "user_id", "amount", "merchant_id",
                    "merchant_lat", "merchant_lon", "status",
                    "event_time", "ingest_time", "plan",
                    "velocity_5min", "amount_zscore", "geo_speed_kmh",
                    "fraud_score", "is_flagged"
                )
                .write.format("starrocks")
                .option("starrocks.fenodes", f"{SR_HOST}:8030")
                .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{SR_HOST}:9030")
                .option("starrocks.table.identifier", f"{SR_DB}.transactions")
                .option("starrocks.user", SR_USER)
                .option("starrocks.password", SR_PASSWORD)
                .mode("append").save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "sr_transactions", "batch_id": batch_id, "error": str(e)}))

        # ── STARROCKS — fraud_scores ──────────────────────────────
        try:
            (
                batch_df.filter(col("is_flagged"))
                .select("transaction_id", "user_id", "fraud_score", "reasons",
                        col("ingest_time").alias("flagged_at"),
                        lit(None).cast("boolean").alias("reviewed"))
                .write.format("starrocks")
                .option("starrocks.fenodes", f"{SR_HOST}:8030")
                .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{SR_HOST}:9030")
                .option("starrocks.table.identifier", f"{SR_DB}.fraud_scores")
                .option("starrocks.user", SR_USER)
                .option("starrocks.password", SR_PASSWORD)
                .mode("append").save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "sr_fraud_scores", "batch_id": batch_id, "error": str(e)}))

        # ── STARROCKS — risk_profiles (latest location per user) ──
        try:
            (
                batch_df
                .withColumn("rn", expr(
                    "row_number() over (partition by user_id order by event_time desc)"
                ))
                .filter(col("rn") == 1)
                .select(
                    "user_id",
                    lit(None).cast("double").alias("avg_amount_30d"),
                    lit(None).cast("double").alias("stddev_amount"),
                    lit(None).cast("double").alias("avg_velocity_1h"),
                    col("merchant_lat").alias("last_merchant_lat"),
                    col("merchant_lon").alias("last_merchant_lon"),
                    col("event_time").alias("updated_at")
                )
                .write.format("starrocks")
                .option("starrocks.fenodes", f"{SR_HOST}:8030")
                .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{SR_HOST}:9030")
                .option("starrocks.table.identifier", f"{SR_DB}.risk_profiles")
                .option("starrocks.user", SR_USER)
                .option("starrocks.password", SR_PASSWORD)
                .mode("append").save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "sr_risk_profiles", "batch_id": batch_id, "error": str(e)}))

        # ── KAFKA — fraud alerts (keyed by user_id for ordering) ──
        try:
            (
                batch_df.filter(col("is_flagged"))
                .select(
                    col("user_id").cast("string").alias("key"),
                    to_json(struct(
                        col("transaction_id"), col("user_id"), col("amount"),
                        col("fraud_score"), col("reasons"),
                        col("ingest_time").cast("string").alias("flagged_at")
                    )).alias("value")
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP)
                .option("topic", TOPIC_ALERTS)
                .options(**_KAFKA_SSL)
                .save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "kafka_alerts", "batch_id": batch_id, "error": str(e)}))

        # ── KAFKA — clean stream (keyed by user_id for ordering) ──
        try:
            (
                batch_df.select(
                    col("user_id").cast("string").alias("key"),
                    to_json(struct(
                        "transaction_id", "user_id", "amount", "merchant_id",
                        "status", "event_time", "plan", "fraud_score", "is_flagged"
                    )).alias("value")
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP)
                .option("topic", TOPIC_CLEAN)
                .options(**_KAFKA_SSL)
                .save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "kafka_clean", "batch_id": batch_id, "error": str(e)}))

        # ── STRUCTURED LOG ────────────────────────────────────────
        logging.warning(json.dumps({
            "batch_id":      batch_id,
            "total":         total,
            "flagged":       flagged,
            "geo_hits":      geo_hits,
            "acc_total":     acc_total.value,
            "acc_flagged":   acc_flagged.value,
            "acc_geo_hits":  acc_geo_hits.value
        }))

    finally:
        batch_df.unpersist()

# ── COLD SINKS — Iceberg (5min trigger, MERGE exactly-once) ──────────────
def write_cold_sinks(batch_df, batch_id: int):
    """Iceberg cold path — MERGE on (k_topic, k_partition, k_offset) ensures
    exactly-once across crash/replay. expire_snapshots(retain_last=1) keeps
    metadata at KiB-scale steady-state."""
    if batch_df.isEmpty():
        return

    # ── VELOCITY SCORE ────────────────────────────────────────────
    batch_df = batch_df.withColumn("score_velocity",
        when(col("velocity_5min") > VELOCITY_THRESHOLD, lit(40)).otherwise(lit(0)))

    # ── CUSTOMER ENRICHMENT ───────────────────────────────────────
    customers_df = get_customers_df()
    batch_df = (
        batch_df
        .join(broadcast(customers_df), batch_df.user_id == customers_df.c_user_id, "left")
        .drop("c_user_id")
        .withColumn("plan",
            when(col("plan").isNull(), lit("unknown")).otherwise(col("plan")))
        .withColumn("avg_amount_30d",
            when(col("avg_amount_30d").isNull(), lit(0.0)).otherwise(col("avg_amount_30d")))
    )

    # ── ZSCORE ────────────────────────────────────────────────────
    batch_df = (
        batch_df
        .withColumn("amount_zscore",
            when(col("avg_amount_30d") > 0,
                (col("amount") - col("avg_amount_30d")) / (col("avg_amount_30d") * 0.3))
            .otherwise(lit(0.0)))
        .withColumn("score_zscore",
            when(col("amount_zscore") > ZSCORE_THRESHOLD, lit(30)).otherwise(lit(0)))
    )

    # ── GEO ENRICHMENT ────────────────────────────────────────────
    refresh_risk_if_stale()
    batch_df = (
        batch_df
        .withColumn("geo_speed_kmh",
            compute_geo_udf(
                col("user_id"), col("merchant_lat"),
                col("merchant_lon"), col("event_time")
            ))
        .withColumn("score_geo",
            when(col("geo_speed_kmh") > GEO_SPEED_THRESHOLD, lit(30)).otherwise(lit(0)))
    )

    # ── FINAL SCORE ───────────────────────────────────────────────
    batch_df = (
        batch_df
        .withColumn("fraud_score",
            col("score_velocity") + col("score_zscore") + col("score_geo"))
        .withColumn("is_flagged", col("fraud_score") >= FRAUD_SCORE_CUTOFF)
        .withColumn("reasons", expr("""
            concat_ws(',',
              case when score_velocity > 0 then 'HIGH_VELOCITY' end,
              case when score_zscore   > 0 then 'AMOUNT_ANOMALY' end,
              case when score_geo      > 0 then 'GEO_IMPOSSIBLE' end
            )
        """))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # ── MERGE transactions_lake (exactly-once by Kafka offsets) ──
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
    except Exception as e:
        logging.error(json.dumps({"event": "sink_failed", "sink": "iceberg_txn_lake", "batch_id": batch_id, "error": str(e)}))

    # ── MERGE processed_batches (exactly-once by batch_id) ────────
    try:
        spark.sql(f"""
            MERGE INTO iceberg.fraud.processed_batches AS t
            USING (SELECT CAST({batch_id} AS BIGINT) AS batch_id,
                          current_timestamp() AS processed_at) AS s
            ON t.batch_id = s.batch_id
            WHEN NOT MATCHED THEN INSERT *
        """)
    except Exception as e:
        logging.error(json.dumps({"event": "sink_failed", "sink": "iceberg_processed_batches", "batch_id": batch_id, "error": str(e)}))

    # ── EXPIRE SNAPSHOTS (steady-state: 1 snapshot per table) ─────
    try:
        spark.sql("CALL iceberg.system.expire_snapshots('fraud.transactions_lake', retain_last => 1)")
    except Exception as e:
        logging.error(json.dumps({"event": "expire_snapshots_failed", "table": "transactions_lake", "error": str(e)}))
    try:
        spark.sql("CALL iceberg.system.expire_snapshots('fraud.processed_batches', retain_last => 1)")
    except Exception as e:
        logging.error(json.dumps({"event": "expire_snapshots_failed", "table": "processed_batches", "error": str(e)}))

    batch_df.unpersist()

# ── DLQ SINK ──────────────────────────────────────────────────────────
dlq_out = bad.select(
    to_json(struct(
        col("dlq_reason").alias("reason"),
        col("k_topic").alias("topic"),
        col("k_partition").alias("partition"),
        col("k_offset").alias("offset"),
        current_timestamp().cast("string").alias("ingest_time"),
        col("raw_json")
    )).alias("value")
)

dlq_query = (
    dlq_out
    .select(col("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("topic", TOPIC_DLQ)
    .options(**_KAFKA_SSL)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/dlq")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

# ── HOT SINK (10s trigger) ─────────────────────────────────
hot_query = (
    final_df.writeStream
    .foreachBatch(write_hot_sinks)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/main")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

# ── COLD SINK (5min trigger, Iceberg MERGE + expire) ──────
cold_query = (
    final_df.writeStream
    .foreachBatch(write_cold_sinks)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/cold")
    .trigger(processingTime=COLD_TRIGGER_INTERVAL)
    .start()
)

spark.streams.awaitAnyTermination()
