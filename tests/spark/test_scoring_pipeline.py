"""End-to-end scoring logic of the streaming job, run on a local SparkSession.

The exact DataFrame transformations from spark-jobs/fraud_stream_to_starrocks.py
(validation/DLQ tagging, customer enrichment + velocity/z-score rules, and the
per-batch geo scoring + final fraud_score/reasons) are compiled from the job's
source and applied to small in-memory DataFrames standing in for the Kafka
source and Iceberg tables. Skipped when pyspark/Java are unavailable.
"""
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

pyspark = pytest.importorskip("pyspark")

from huanca_testutils import SCORER_PATH, exec_statements, find_assign, load_symbols  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql.functions import udf  # noqa: E402
from pyspark.sql.types import DoubleType  # noqa: E402

PYSPARK_IMPORTS = ("pyspark", "math", "json", "logging", "os", "time")
REQUIRED_ENV = {
    "REDPANDA_BOOTSTRAP": "unused:9093",
    "STARROCKS_PASSWORD": "unused",
    "MINIO_ACCESS_KEY": "unused",
    "MINIO_SECRET_KEY": "unused",
    "ICEBERG_DB_PASSWORD": "unused",
}
NY = (40.7128, -74.0060)
LA = (34.0522, -118.2437)
T0 = datetime(2026, 3, 24, 12, 0, 0)


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("huanca-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def job_env(monkeypatch):
    for k, v in REQUIRED_ENV.items():
        monkeypatch.setenv(k, v)


# ── Validation / DLQ routing ───────────────────────────────────────────
def _route(spark, messages):
    raw_with_meta = spark.createDataFrame(
        [("transactions-raw", 0, i, m) for i, m in enumerate(messages)],
        "k_topic STRING, k_partition INT, k_offset LONG, raw_json STRING",
    )
    ns = load_symbols(
        SCORER_PATH,
        ["txn_schema", "parsed", "expanded", "dlq_reason", "tagged", "bad", "good"],
        namespace={"raw_with_meta": raw_with_meta},
        import_prefixes=PYSPARK_IMPORTS,
    )
    reasons = {r["k_offset"]: r["dlq_reason"] for r in ns["bad"].collect()}
    good = {r["k_offset"]: r for r in ns["good"].collect()}
    return reasons, good, ns["good"].columns


def _msg(**overrides):
    base = {
        "transaction_id": "t-1",
        "user_id": "user_001",
        "amount": 25.0,
        "merchant_id": "merchant_0001",
        "merchant_lat": NY[0],
        "merchant_lon": NY[1],
        "status": "pending",
        "timestamp": "2026-03-24T12:00:00Z",
    }
    base.update(overrides)
    return json.dumps({k: v for k, v in base.items() if v is not ...})


def test_valid_record_passes_validation(spark, job_env):
    reasons, good, columns = _route(spark, [_msg()])
    assert reasons == {}
    row = good[0]
    assert row["transaction_id"] == "t-1" and row["amount"] == 25.0
    # collect() returns naive local-time datetimes; normalise before comparing.
    assert row["event_time"].astimezone(timezone.utc) == datetime(2026, 3, 24, 12, 0, 0, tzinfo=timezone.utc)
    assert "dlq_reason" not in columns and "j" not in columns


@pytest.mark.parametrize(
    "message,reason",
    [
        (_msg(transaction_id=...), "missing_transaction_id"),
        (_msg(transaction_id=""), "missing_transaction_id"),
        (_msg(user_id=...), "missing_user_id"),
        (_msg(user_id=""), "missing_user_id"),
        (_msg(amount=...), "missing_amount"),
        (_msg(amount=0), "non_positive_amount"),
        (_msg(amount=-5.5), "non_positive_amount"),
        (_msg(merchant_id=...), "missing_merchant_id"),
        (_msg(merchant_id=""), "missing_merchant_id"),
        (_msg(timestamp="not-a-date"), "bad_timestamp"),
        (_msg(timestamp=...), "bad_timestamp"),
    ],
    ids=[
        "no-txn-id", "empty-txn-id", "no-user", "empty-user", "no-amount", "zero-amount",
        "negative-amount", "no-merchant", "empty-merchant", "bad-timestamp", "no-timestamp",
    ],
)
def test_invalid_records_routed_to_dlq_with_reason(spark, job_env, message, reason):
    reasons, good, _ = _route(spark, [message])
    assert reasons == {0: reason}
    assert good == {}


def test_dlq_reason_precedence(spark, job_env):
    # Several defects at once: the first rule in the chain wins.
    reasons, _, _ = _route(spark, [_msg(transaction_id="", user_id="", amount=0)])
    assert reasons == {0: "missing_transaction_id"}


def test_unparseable_payloads_never_reach_scoring(spark, job_env):
    reasons, good, _ = _route(spark, ["not json at all", "{\"amount\": \"abc\"}", ""])
    assert good == {}
    assert set(reasons) == {0, 1, 2}
    assert all(reasons.values())


def test_mixed_batch_is_split(spark, job_env):
    reasons, good, _ = _route(spark, [_msg(), _msg(amount=-1), _msg(transaction_id="t-2")])
    assert set(good) == {0, 2}
    assert reasons == {1: "non_positive_amount"}


# ── Rule-based scoring ────────────────────────────────────────────────
VELOCITY_COLS = (
    "transaction_id STRING, user_id STRING, amount DOUBLE, merchant_id STRING, "
    "merchant_lat DOUBLE, merchant_lon DOUBLE, status STRING, event_time TIMESTAMP, "
    "ingest_time TIMESTAMP, velocity_5min LONG, raw_json STRING, k_topic STRING, "
    "k_partition LONG, k_offset LONG"
)


def _vel_row(txn_id, user, amount, velocity, lat=NY[0], lon=NY[1], when=T0, offset=0):
    return (txn_id, user, amount, "m", lat, lon, "pending", when, when, velocity, "{}", "transactions-raw", 0, offset)


def _score(spark, rows, customers, risk_profiles=None):
    velocity_df = spark.createDataFrame(rows, VELOCITY_COLS)
    customers_df = spark.createDataFrame(customers, "c_user_id STRING, plan STRING, avg_amount_30d DOUBLE")
    ns = load_symbols(
        SCORER_PATH,
        ["VELOCITY_THRESHOLD", "ZSCORE_THRESHOLD", "GEO_SPEED_THRESHOLD", "FRAUD_SCORE_CUTOFF",
         "haversine", "compute_geo", "final_df"],
        namespace={
            "velocity_df": velocity_df,
            "customers_df": customers_df,
            "risk_bc": SimpleNamespace(value=risk_profiles or {}),
        },
        import_prefixes=PYSPARK_IMPORTS,
    )
    ns["compute_geo_udf"] = udf(ns["compute_geo"], DoubleType())
    ns["batch_df"] = ns["final_df"]
    exec_statements(SCORER_PATH, [find_assign(SCORER_PATH, "batch_df", inside_function="write_hot_sinks")], ns)
    scored = {r["transaction_id"]: r.asDict() for r in ns["batch_df"].collect()}
    ns["batch_df"].unpersist()
    return scored


CUSTOMERS = [("u_known", "PREMIUM", 100.0)]


def test_clean_transaction_scores_zero(spark, job_env):
    out = _score(spark, [_vel_row("t", "u_known", 110.0, velocity=1)], CUSTOMERS)["t"]
    assert out["fraud_score"] == 0
    assert out["is_flagged"] is False
    assert out["reasons"] == ""
    assert out["plan"] == "PREMIUM"
    assert out["amount_zscore"] == pytest.approx((110 - 100) / 30)


def test_velocity_threshold_is_strictly_greater_than(spark, job_env):
    out = _score(
        spark,
        [_vel_row("at", "u_known", 100.0, velocity=5), _vel_row("over", "u_known", 100.0, velocity=6, offset=1)],
        CUSTOMERS,
    )
    assert out["at"]["score_velocity"] == 0
    assert out["over"]["score_velocity"] == 40
    assert out["over"]["fraud_score"] == 40
    assert out["over"]["is_flagged"] is False  # velocity alone is below the 60 cutoff
    assert out["over"]["reasons"] == "HIGH_VELOCITY"


def test_velocity_plus_amount_anomaly_is_flagged(spark, job_env):
    out = _score(spark, [_vel_row("t", "u_known", 1000.0, velocity=7)], CUSTOMERS)["t"]
    assert out["amount_zscore"] == pytest.approx(30.0)
    assert out["fraud_score"] == 70
    assert out["is_flagged"] is True
    assert out["reasons"] == "HIGH_VELOCITY,AMOUNT_ANOMALY"


def test_amount_anomaly_plus_impossible_travel_hits_cutoff_exactly(spark, job_env):
    profiles = {"u_known": {"last_merchant_lat": NY[0], "last_merchant_lon": NY[1], "updated_at": T0}}
    row = _vel_row("t", "u_known", 500.0, velocity=1, lat=LA[0], lon=LA[1], when=T0 + timedelta(hours=1))
    out = _score(spark, [row], CUSTOMERS, profiles)["t"]
    assert out["geo_speed_kmh"] == pytest.approx(3936, rel=0.01)
    assert out["score_geo"] == 30 and out["score_zscore"] == 30
    assert out["fraud_score"] == 60
    assert out["is_flagged"] is True  # cutoff is inclusive (>= 60)
    assert out["reasons"] == "AMOUNT_ANOMALY,GEO_IMPOSSIBLE"


def test_all_signals_max_score(spark, job_env):
    profiles = {"u_known": {"last_merchant_lat": NY[0], "last_merchant_lon": NY[1], "updated_at": T0}}
    row = _vel_row("t", "u_known", 900.0, velocity=12, lat=LA[0], lon=LA[1], when=T0 + timedelta(minutes=30))
    out = _score(spark, [row], CUSTOMERS, profiles)["t"]
    assert out["fraud_score"] == 100
    assert out["reasons"] == "HIGH_VELOCITY,AMOUNT_ANOMALY,GEO_IMPOSSIBLE"


def test_unknown_customer_gets_defaults_and_no_zscore(spark, job_env):
    out = _score(spark, [_vel_row("t", "u_new", 99999.0, velocity=1)], CUSTOMERS)["t"]
    assert out["plan"] == "unknown"
    assert out["avg_amount_30d"] == 0.0
    assert out["amount_zscore"] == 0.0  # no baseline → no anomaly signal (no division by zero)
    assert out["fraud_score"] == 0


def test_cold_start_without_risk_profile_has_no_geo_signal(spark, job_env):
    row = _vel_row("t", "u_known", 100.0, velocity=1, lat=LA[0], lon=LA[1])
    out = _score(spark, [row], CUSTOMERS, risk_profiles={})["t"]
    assert out["geo_speed_kmh"] == 0.0
    assert out["score_geo"] == 0


def test_thresholds_are_overridable_via_env(spark, job_env, monkeypatch):
    monkeypatch.setenv("FRAUD_VELOCITY_THRESHOLD", "2")
    monkeypatch.setenv("FRAUD_SCORE_CUTOFF", "40")
    out = _score(spark, [_vel_row("t", "u_known", 100.0, velocity=3)], CUSTOMERS)["t"]
    assert out["score_velocity"] == 40
    assert out["is_flagged"] is True
