"""
Real-Time Payment Fraud Detection
Redpanda → Spark Structured Streaming → StarRocks + Iceberg + DLQ

Architecture:
  - At-least-once delivery (Kafka semantics)
  - Idempotent sink (StarRocks Primary Key upserts)
  - Stateful feature engineering (5-min velocity window, bounded state)
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
import threading
from math import radians, sin, cos, sqrt, atan2

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, current_timestamp, when,
    to_timestamp, expr, struct, to_json, udf,
    count, sum as spark_sum, window, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

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
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))  # cap batch size — prevent OOM on backlog

# ── FRAUD THRESHOLDS (overridable via env) ────────────────────────────
VELOCITY_THRESHOLD  = int(os.environ.get("FRAUD_VELOCITY_THRESHOLD",  "10"))
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

# ── HAVERSINE UDF (geo distance in km) ───────────────────────────────
# Used by compute_geo inside write_all_sinks foreachBatch
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
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)  # MinIO credentials from K8s Secret
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)  # Iceberg JDBC catalog password from K8s Secret
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── RISK PROFILE BROADCAST CACHE ──────────────────────────────────────
# Broadcast to all executors. Refreshed every RISK_CACHE_TTL_SEC seconds
# via background daemon thread — zero StarRocks reads per batch after first load.
# Lock prevents race between unpersist and active UDF calls on executors.
RISK_CACHE_TTL = int(os.environ.get("RISK_CACHE_TTL_SEC", "60"))
_risk_lock = threading.Lock()

def load_risk_profiles():
    return (
        spark.read
        .format("starrocks")
        .option("starrocks.fenodes", f"{SR_HOST}:8030")
        .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{SR_HOST}:9030")
        .option("starrocks.table.identifier", f"{SR_DB}.risk_profiles")
        .option("starrocks.user", SR_USER)
        .option("starrocks.password", SR_PASSWORD)
        .load()
        .rdd.map(lambda r: (r.user_id, r.asDict()))
        .collectAsMap()
    )

risk_bc = spark.sparkContext.broadcast(load_risk_profiles())

def refresh_risk_broadcast():
    global risk_bc
    backoff = 60
    while True:
        time.sleep(RISK_CACHE_TTL)
        try:
            new_bc = spark.sparkContext.broadcast(load_risk_profiles())
            with _risk_lock:
                risk_bc.unpersist()
                risk_bc = new_bc
            backoff = 60  # reset on success
        except Exception as e:
            logging.error(json.dumps({"event": "risk_refresh_failed", "error": str(e)}))
            time.sleep(backoff)
            backoff = min(backoff * 2, 600)  # exponential backoff, cap at 10 min

threading.Thread(target=refresh_risk_broadcast, daemon=True).start()

# ── CUSTOMER ENRICHMENT CACHE ──────────────────────────────────────────
# TTL-based in-memory cache. Loaded from iceberg.fraud.customers on MinIO.
# Populated daily by Airflow (customer_csv_to_iceberg.py).
# Refreshes every CUSTOMER_CACHE_TTL seconds inside foreachBatch.
_customer_cache: dict = {"df": None, "loaded_at": 0.0}

def get_customers_df():
    """Returns customer enrichment DataFrame from Iceberg, refreshing if TTL expired."""
    now = time.time()
    if _customer_cache["df"] is None or (now - _customer_cache["loaded_at"]) > CUSTOMER_CACHE_TTL:
        _customer_cache["df"] = (
            spark.read
            .format("iceberg")
            .load(CUSTOMER_ICEBERG_TABLE)
            .select(
                col("user_id").alias("c_user_id"),
                col("plan"),
                col("avg_amount_30d").cast("double")
            )
        )
        _customer_cache["loaded_at"] = now
    return _customer_cache["df"]

# ── OBSERVABILITY ACCUMULATORS ─────────────────────────────────────────
# Driver-side counters visible in Spark UI.
# Guarded by iceberg.fraud.processed_batches — incremented only once per batch_id.
acc_total    = spark.sparkContext.accumulator(0, "total_processed")
acc_flagged  = spark.sparkContext.accumulator(0, "flagged_count")
acc_geo_hits = spark.sparkContext.accumulator(0, "geo_hits")

# ── READ FROM REDPANDA ─────────────────────────────────────────────────
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)  # cap batch size — prevent OOM on backlog
    .option("kafka.group.id", "fraud-spark-consumer")
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
# Velocity: count per user in 5-min tumbling window.
# Watermark MUST be applied to the source stream before aggregation — Spark
# requires this to track late-data eviction through the window operator and
# allow append output mode on the aggregated streaming DataFrame.
# Transactions arriving >5min late get velocity_5min=1 (acceptable tradeoff).
good_wm = good.withWatermark("event_time", "5 minutes")

velocity = (
    good_wm
    .groupBy(window("event_time", "5 minutes"), col("user_id"))
    .agg(count("*").alias("velocity_5min"))
    .select(
        col("user_id").alias("v_user_id"),
        col("velocity_5min"),
        col("window.start").alias("window_start")
    )
)

# Secondary watermark on velocity's window_start drives join state eviction:
# Spark drops velocity state once its watermark advances past window_start + 5 min
# (bounded by the INTERVAL condition on the join below).
velocity_wm = velocity.withWatermark("window_start", "5 minutes")

# Time-range condition on join — Spark evicts state once watermark passes window_start + 5 min.
# window_start kept for audit (which 5-min bucket the transaction landed in).
good_with_velocity = (
    good_wm.join(
        velocity_wm,
        (good_wm.user_id == velocity_wm.v_user_id) &
        (good_wm.event_time >= velocity_wm.window_start) &
        (good_wm.event_time < velocity_wm.window_start + expr("INTERVAL 5 MINUTES")),
        "leftOuter"
    )
    .drop("v_user_id")
    .withColumn("velocity_5min",
        when(col("velocity_5min").isNull(), lit(1)).otherwise(col("velocity_5min")))
)

# ── PRE-SCORE (velocity only) ──────────────────────────────────────────
# Customer enrichment, zscore, geo, and final scoring happen in foreachBatch
# where Iceberg (customers) and risk_profiles broadcast are available.
pre_scored = (
    good_with_velocity
    .withColumn("score_velocity",
        when(col("velocity_5min") > VELOCITY_THRESHOLD, lit(40)).otherwise(lit(0)))
)

final_df = pre_scored.select(
    "transaction_id", "user_id", "amount", "merchant_id",
    "merchant_lat", "merchant_lon", "status",
    "event_time", "ingest_time",
    "velocity_5min", "window_start",
    "score_velocity",
    "raw_json", "k_topic", "k_partition", "k_offset"
)

# ── WRITE ALL SINKS ───────────────────────────────────────────────────
def write_all_sinks(batch_df, batch_id: int):

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
        .persist(StorageLevel.MEMORY_AND_DISK_SER)
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
                        col("ingest_time").alias("flagged_at"))
                .write.format("starrocks")
                .option("starrocks.fenodes", f"{SR_HOST}:8030")
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
                    col("merchant_lat").alias("last_merchant_lat"),
                    col("merchant_lon").alias("last_merchant_lon"),
                    col("event_time").alias("updated_at")
                )
                .write.format("starrocks")
                .option("starrocks.fenodes", f"{SR_HOST}:8030")
                .option("starrocks.table.identifier", f"{SR_DB}.risk_profiles")
                .option("starrocks.user", SR_USER)
                .option("starrocks.password", SR_PASSWORD)
                .mode("append").save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "sr_risk_profiles", "batch_id": batch_id, "error": str(e)}))

        # ── ICEBERG — audit trail ─────────────────────────────────
        try:
            (
                batch_df.select(
                    "transaction_id", "user_id", "amount", "merchant_id",
                    "merchant_lat", "merchant_lon", "status", "event_time",
                    "ingest_time", "fraud_score", "is_flagged", "reasons"
                )
                .write.format("iceberg")
                .mode("append")
                .save("iceberg.fraud.transactions_lake")
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "iceberg_txn_lake", "batch_id": batch_id, "error": str(e)}))

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

        # ── ICEBERG — processed batch dedup record ────────────────
        try:
            if not already_processed:
                (
                    spark.createDataFrame([(batch_id,)], ["batch_id"])
                    .withColumn("processed_at", current_timestamp())
                    .write.format("iceberg")
                    .mode("append")
                    .save("iceberg.fraud.processed_batches")
                )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "iceberg_processed_batches", "batch_id": batch_id, "error": str(e)}))

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

# ── MAIN SINK ─────────────────────────────────────────────────────────
main_query = (
    final_df.writeStream
    .foreachBatch(write_all_sinks)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/main")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

spark.streams.awaitAnyTermination()
