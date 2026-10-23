"""FastAPI backend (k8s-apps/backend-api/main.py).

Kafka (confluent-kafka Producer) and StarRocks (mysql-connector pool) are
replaced with mocks before the module is imported, so no broker or database is
needed. Everything else — routing, auth, validation, payload construction,
delivery-failure handling, SQL parameter binding, connection cleanup — is the
production code path.
"""
import importlib.util
import json
import uuid
from datetime import datetime
from unittest.mock import MagicMock

import pytest
from confluent_kafka import KafkaException
from fastapi.testclient import TestClient
from huanca_testutils import BACKEND_PATH

API_KEY = "test-api-key"
AUTH = {"X-Api-Key": API_KEY}


@pytest.fixture
def backend(monkeypatch):
    monkeypatch.setenv("REDPANDA_BOOTSTRAP", "redpanda:9093")
    monkeypatch.setenv("STARROCKS_PASSWORD", "unused")
    monkeypatch.setenv("API_KEY", API_KEY)
    monkeypatch.setenv("TOPIC_RAW", "transactions-raw-test")

    producer = MagicMock(name="Producer")
    producer.flush.return_value = 0
    pool = MagicMock(name="MySQLConnectionPool")

    import confluent_kafka
    from mysql.connector import pooling

    monkeypatch.setattr(confluent_kafka, "Producer", MagicMock(return_value=producer))
    monkeypatch.setattr(pooling, "MySQLConnectionPool", MagicMock(return_value=pool))

    spec = importlib.util.spec_from_file_location("huanca_backend_main", BACKEND_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    module.test_producer = producer
    module.test_pool = pool
    return module


@pytest.fixture
def client(backend):
    return TestClient(backend.app)


def _cursor(pool, *, fetchall=None, fetchone=None):
    conn = MagicMock(name="conn")
    cur = MagicMock(name="cursor")
    cur.fetchall.return_value = fetchall
    cur.fetchone.return_value = fetchone
    conn.cursor.return_value = cur
    pool.get_connection.return_value = conn
    return conn, cur


VALID_TXN = {
    "user_id": "user_001",
    "amount": 42.5,
    "merchant_id": "merchant_0007",
    "merchant_lat": 40.71,
    "merchant_lon": -74.0,
}


# ── Startup wiring ─────────────────────────────────────────────────────
def test_producer_configured_for_durable_tls_delivery(backend):
    import confluent_kafka

    cfg = confluent_kafka.Producer.call_args.args[0]
    assert cfg["bootstrap.servers"] == "redpanda:9093"
    assert cfg["acks"] == "all"
    assert cfg["security.protocol"] == "SSL"


def test_module_fails_fast_without_required_env(monkeypatch):
    monkeypatch.delenv("API_KEY", raising=False)
    monkeypatch.setenv("REDPANDA_BOOTSTRAP", "x")
    monkeypatch.setenv("STARROCKS_PASSWORD", "x")
    spec = importlib.util.spec_from_file_location("huanca_backend_noenv", BACKEND_PATH)
    module = importlib.util.module_from_spec(spec)
    with pytest.raises(KeyError, match="API_KEY"):
        spec.loader.exec_module(module)


# ── Health ─────────────────────────────────────────────────────────────
def test_health_requires_no_auth_and_reports_ok(client, backend):
    backend.test_producer.list_topics.return_value = MagicMock()
    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["service"] == "fraud-api"
    datetime.fromisoformat(body["time"])
    backend.test_pool.get_connection.return_value.close.assert_called_once()


def test_health_503_when_redpanda_unreachable(client, backend):
    backend.test_producer.list_topics.side_effect = KafkaException("broker down")
    resp = client.get("/api/health")
    assert resp.status_code == 503
    assert resp.json()["detail"] == {"redpanda": False, "starrocks": True}


def test_health_503_when_starrocks_unreachable(client, backend):
    backend.test_pool.get_connection.side_effect = RuntimeError("pool exhausted")
    resp = client.get("/api/health")
    assert resp.status_code == 503
    assert resp.json()["detail"] == {"redpanda": True, "starrocks": False}


# ── Auth ───────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "method,path",
    [
        ("post", "/api/transaction"),
        ("get", "/api/fraud-scores"),
        ("get", "/api/stats"),
        ("get", "/api/top-risky-users"),
    ],
)
@pytest.mark.parametrize("headers", [{}, {"X-Api-Key": "wrong"}])
def test_protected_routes_reject_missing_or_bad_key(client, backend, method, path, headers):
    resp = client.request(method.upper(), path, headers=headers, json=VALID_TXN if method == "post" else None)
    assert resp.status_code == 403
    backend.test_producer.produce.assert_not_called()
    backend.test_pool.get_connection.assert_not_called()


# ── POST /api/transaction ──────────────────────────────────────────────
def test_transaction_is_produced_and_acknowledged(client, backend):
    resp = client.post("/api/transaction", json=VALID_TXN, headers=AUTH)

    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "queued"
    assert uuid.UUID(body["transaction_id"]).version == 4

    producer = backend.test_producer
    producer.produce.assert_called_once()
    producer.flush.assert_called_once_with(timeout=5)
    (topic,), kwargs = producer.produce.call_args
    assert topic == "transactions-raw-test"
    payload = json.loads(kwargs["value"].decode("utf-8"))
    assert payload["transaction_id"] == body["transaction_id"]
    for key, value in VALID_TXN.items():
        assert payload[key] == value
    assert payload["status"] == "pending"
    assert payload["timestamp"].endswith("Z")
    datetime.fromisoformat(payload["timestamp"][:-1])


def test_each_transaction_gets_a_unique_id(client):
    ids = {client.post("/api/transaction", json=VALID_TXN, headers=AUTH).json()["transaction_id"] for _ in range(5)}
    assert len(ids) == 5


def test_optional_geo_fields_default_to_null(client, backend):
    txn = {"user_id": "u", "amount": 1.0, "merchant_id": "m"}
    assert client.post("/api/transaction", json=txn, headers=AUTH).status_code == 202
    payload = json.loads(backend.test_producer.produce.call_args.kwargs["value"])
    assert payload["merchant_lat"] is None and payload["merchant_lon"] is None


@pytest.mark.parametrize(
    "bad",
    [
        {**VALID_TXN, "amount": 0},
        {**VALID_TXN, "amount": -10.0},
        {**VALID_TXN, "amount": "lots"},
        {k: v for k, v in VALID_TXN.items() if k != "user_id"},
        {k: v for k, v in VALID_TXN.items() if k != "merchant_id"},
        {k: v for k, v in VALID_TXN.items() if k != "amount"},
    ],
    ids=["zero", "negative", "non-numeric", "no-user", "no-merchant", "no-amount"],
)
def test_invalid_transactions_rejected_before_produce(client, backend, bad):
    resp = client.post("/api/transaction", json=bad, headers=AUTH)
    assert resp.status_code == 422
    backend.test_producer.produce.assert_not_called()


def test_delivery_error_returns_503(client, backend):
    def produce(topic, value, callback):
        callback("broker rejected", None)

    backend.test_producer.produce.side_effect = produce
    resp = client.post("/api/transaction", json=VALID_TXN, headers=AUTH)
    assert resp.status_code == 503
    assert "retry" in resp.json()["detail"]


def test_flush_timeout_returns_503(client, backend):
    backend.test_producer.flush.return_value = 1  # one message still in queue
    resp = client.post("/api/transaction", json=VALID_TXN, headers=AUTH)
    assert resp.status_code == 503


# ── Query endpoints ────────────────────────────────────────────────────
def test_fraud_scores_returns_rows_and_binds_limit(client, backend):
    rows = [{"transaction_id": "t1", "user_id": "u1", "fraud_score": 70, "reasons": "HIGH_VELOCITY", "flagged_at": "x"}]
    conn, cur = _cursor(backend.test_pool, fetchall=rows)

    resp = client.get("/api/fraud-scores?limit=50", headers=AUTH)

    assert resp.status_code == 200
    assert resp.json() == {"fraud_scores": rows}
    sql, params = cur.execute.call_args.args
    assert "FROM fraud_scores" in sql and "LIMIT %s" in sql
    assert params == (50,)
    conn.close.assert_called_once()


def test_fraud_scores_default_limit(client, backend):
    _, cur = _cursor(backend.test_pool, fetchall=[])
    client.get("/api/fraud-scores", headers=AUTH)
    assert cur.execute.call_args.args[1] == (20,)


@pytest.mark.parametrize("path", ["/api/fraud-scores", "/api/top-risky-users"])
@pytest.mark.parametrize("limit", ["0", "1001", "abc"])
def test_limit_bounds_enforced(client, backend, path, limit):
    resp = client.get(f"{path}?limit={limit}", headers=AUTH)
    assert resp.status_code == 422
    backend.test_pool.get_connection.assert_not_called()


def test_connection_released_when_query_fails(client, backend):
    conn, cur = _cursor(backend.test_pool)
    cur.execute.side_effect = RuntimeError("starrocks error")
    with pytest.raises(RuntimeError):
        client.get("/api/fraud-scores", headers=AUTH)
    conn.close.assert_called_once()


def test_stats_returns_single_row(client, backend):
    stats = {"total_transactions": 10, "flagged_count": 2, "avg_fraud_score": 12.5, "last_ingest": None}
    conn, cur = _cursor(backend.test_pool, fetchone=stats)
    resp = client.get("/api/stats", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == stats
    assert "interval 1 hour" in cur.execute.call_args.args[0]
    conn.close.assert_called_once()


def test_top_risky_users(client, backend):
    rows = [{"user_id": "u1", "flagged_count": 3, "max_score": 100}]
    conn, cur = _cursor(backend.test_pool, fetchall=rows)
    resp = client.get("/api/top-risky-users?limit=5", headers=AUTH)
    assert resp.status_code == 200
    assert resp.json() == {"top_risky_users": rows}
    sql, params = cur.execute.call_args.args
    assert "interval 24 hour" in sql and "GROUP BY user_id" in sql
    assert params == (5,)
    conn.close.assert_called_once()
