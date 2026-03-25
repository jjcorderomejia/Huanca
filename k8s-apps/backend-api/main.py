"""
Fraud Detection Backend API
- POST /api/transaction      → produce to Redpanda
- GET  /api/fraud-scores     → query StarRocks
- GET  /api/health           → readiness + liveness probe (no auth)
- GET  /api/stats            → dashboard stats
- GET  /api/top-risky-users  → top risky users (24h)

Auth: X-Api-Key header required on all routes except /api/health.
Key loaded from API_KEY env var (K8s Secret: api-key-secret).
The key is visible in browser DevTools by design — this is a machine-to-machine
access control layer that blocks unauthenticated callers. User-facing login (JWT)
is deferred to Phase 8+.
"""
import os
import uuid
import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, APIRouter, Query
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, validator
from mysql.connector import pooling
from confluent_kafka import Producer, KafkaException

# ── Config ─────────────────────────────────────────────────────────
REDPANDA_BOOTSTRAP = os.environ["REDPANDA_BOOTSTRAP"]
STARROCKS_HOST     = os.environ.get("STARROCKS_HOST", "starrocks-fe.bigdata.svc.cluster.local")
STARROCKS_PORT     = int(os.environ.get("STARROCKS_PORT", "9030"))
STARROCKS_DB       = os.environ.get("STARROCKS_DB", "fraud")
STARROCKS_PASSWORD = os.environ["STARROCKS_PASSWORD"]
TOPIC_RAW          = os.environ.get("TOPIC_RAW", "transactions-raw")
API_KEY            = os.environ["API_KEY"]

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":%(message)s}',
)
log = logging.getLogger("fraud-api")

# ── Kafka Producer (confluent-kafka) ────────────────────────────────
producer = Producer({
    "bootstrap.servers":               REDPANDA_BOOTSTRAP,
    "acks":                            "all",
    "retries":                         3,
    "retry.backoff.ms":                200,
    "security.protocol":               "SSL",
    "ssl.ca.location":                 "/etc/redpanda-certs/ca.crt",
    "ssl.endpoint.identification.algorithm": "none",
})

# ── StarRocks Connection Pool ───────────────────────────────────────
db_pool = pooling.MySQLConnectionPool(
    pool_name="sr_pool",
    pool_size=5,
    host=STARROCKS_HOST,
    port=STARROCKS_PORT,
    user="root",
    password=STARROCKS_PASSWORD,
    database=STARROCKS_DB,
)

def get_sr_conn():
    return db_pool.get_connection()

# ── Auth ────────────────────────────────────────────────────────────
api_key_header = APIKeyHeader(name="X-Api-Key", auto_error=False)

def verify_api_key(key: str = Depends(api_key_header)):
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key")
    return key

# ── App + Protected Router ──────────────────────────────────────────
app    = FastAPI(title="Fraud Detection API", version="1.0.0")
router = APIRouter(dependencies=[Depends(verify_api_key)])

# ── Models ──────────────────────────────────────────────────────────
class Transaction(BaseModel):
    user_id:      str
    amount:       float
    merchant_id:  str
    merchant_lat: Optional[float] = None
    merchant_lon: Optional[float] = None
    status:       str = "pending"

    @validator("amount")
    def amount_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("amount must be positive")
        return v

# ── Health — no auth, required for K8s probes ───────────────────────
@app.get("/api/health")
def health():
    redpanda_ok  = False
    starrocks_ok = False

    try:
        meta = producer.list_topics(timeout=2)
        redpanda_ok = meta is not None
    except KafkaException as e:
        log.error('"event":"health_redpanda_fail","error":"%s"', e)

    try:
        conn = get_sr_conn()
        conn.close()
        starrocks_ok = True
    except Exception as e:
        log.error('"event":"health_starrocks_fail","error":"%s"', e)

    if not redpanda_ok or not starrocks_ok:
        raise HTTPException(
            status_code=503,
            detail={"redpanda": redpanda_ok, "starrocks": starrocks_ok},
        )

    return {"status": "ok", "service": "fraud-api", "time": datetime.utcnow().isoformat()}

# ── Protected Routes ─────────────────────────────────────────────────
@router.post("/api/transaction", status_code=202)
def create_transaction(txn: Transaction):
    payload = {
        "transaction_id": str(uuid.uuid4()),   # UUID4 — never $RANDOM
        "user_id":        txn.user_id,
        "amount":         txn.amount,
        "merchant_id":    txn.merchant_id,
        "merchant_lat":   txn.merchant_lat,
        "merchant_lon":   txn.merchant_lon,
        "status":         txn.status,
        "timestamp":      datetime.utcnow().isoformat() + "Z",
    }

    delivery_error = {}

    def on_delivery(err, msg):
        if err:
            delivery_error["err"] = err

    producer.produce(TOPIC_RAW, value=json.dumps(payload).encode("utf-8"), callback=on_delivery)
    remaining = producer.flush(timeout=5)

    if remaining > 0 or delivery_error.get("err"):
        log.error(
            '"event":"produce_fail","transaction_id":"%s","error":"%s"',
            payload["transaction_id"], delivery_error.get("err", "timeout"),
        )
        raise HTTPException(status_code=503, detail="Message delivery failed — retry")

    log.info(
        '"event":"transaction_queued","transaction_id":"%s","user_id":"%s"',
        payload["transaction_id"], txn.user_id,
    )
    return {"transaction_id": payload["transaction_id"], "status": "queued"}

@router.get("/api/fraud-scores")
def get_fraud_scores(limit: int = Query(default=20, ge=1, le=1000)):
    conn = get_sr_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT transaction_id, user_id, fraud_score, reasons, flagged_at
            FROM fraud_scores
            ORDER BY flagged_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return {"fraud_scores": rows}
    finally:
        conn.close()

@router.get("/api/stats")
def get_stats():
    conn = get_sr_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
              count(*)                                        AS total_transactions,
              sum(case when is_flagged then 1 else 0 end)    AS flagged_count,
              avg(fraud_score)                               AS avg_fraud_score,
              max(ingest_time)                               AS last_ingest
            FROM fraud.transactions
            WHERE event_time >= date_sub(now(), interval 1 hour)
        """)
        stats = cur.fetchone()
        cur.close()
        return stats
    finally:
        conn.close()

@router.get("/api/top-risky-users")
def top_risky_users(limit: int = Query(default=10, ge=1, le=1000)):
    conn = get_sr_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT user_id, count(*) AS flagged_count, max(fraud_score) AS max_score
            FROM fraud_scores
            WHERE flagged_at >= date_sub(now(), interval 24 hour)
            GROUP BY user_id
            ORDER BY flagged_count DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return {"top_risky_users": rows}
    finally:
        conn.close()

app.include_router(router)
