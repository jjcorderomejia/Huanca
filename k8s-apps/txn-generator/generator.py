"""
Realistic transaction generator — continuous HTTP producer for the Huanca fraud pipeline.

Produces to POST /api/transaction via the FastAPI backend.
Uses exponential backoff on failures: 1s, 2s, 4s, ... capped at 60s.
Runs indefinitely — deploy as a K8s Deployment with restartPolicy: Always.

Design decisions:
- 50 simulated users with realistic home lat/lon across US cities
- Poisson-distributed inter-arrival times (avg 2 txn/min) — realistic bursts
- 10% chance of fraud pattern per transaction:
    - velocity burst: 6 rapid transactions in 5 minutes
    - amount anomaly: 8–15x the user's typical spend
    - geo-speed violation: merchant location impossible to reach from last transaction
- All amounts, merchants, and locations are seeded per user for consistency
- X-Api-Key injected from API_KEY env var (matches backend-api secret)
"""

import os
import time
import uuid
import random
import math
import logging
import requests
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":%(message)s}'
)
log = logging.getLogger(__name__)

# ── CONFIG ─────────────────────────────────────────────────────────────
BACKEND_URL  = os.environ.get("BACKEND_URL",  "http://backend-api.apps.svc.cluster.local:8000")
API_KEY      = os.environ.get("API_KEY",      "")
TXN_RATE     = float(os.environ.get("TXN_RATE", "6.0"))   # avg transactions per minute
BACKOFF_MAX  = int(os.environ.get("BACKOFF_MAX", "60"))    # max backoff seconds

HEADERS = {"X-Api-Key": API_KEY, "Content-Type": "application/json"}

# ── SIMULATED USERS (50 users, home cities across US) ─────────────────
CITIES = [
    ("New York",      40.7128,  -74.0060),
    ("Los Angeles",   34.0522, -118.2437),
    ("Chicago",       41.8781,  -87.6298),
    ("Houston",       29.7604,  -95.3698),
    ("Phoenix",       33.4484, -112.0740),
    ("Philadelphia",  39.9526,  -75.1652),
    ("San Antonio",   29.4241,  -98.4936),
    ("San Diego",     32.7157, -117.1611),
    ("Dallas",        32.7767,  -96.7970),
    ("San Jose",      37.3382, -121.8863),
]

random.seed(42)
USERS = []
for i in range(50):
    city_name, city_lat, city_lon = CITIES[i % len(CITIES)]
    USERS.append({
        "user_id":      f"user_{i:03d}",
        "home_lat":     city_lat + random.uniform(-0.15, 0.15),
        "home_lon":     city_lon + random.uniform(-0.15, 0.15),
        "avg_spend":    round(random.uniform(20.0, 300.0), 2),
        "last_lat":     None,
        "last_lon":     None,
        "last_time":    None,
    })

MERCHANTS = [f"merchant_{i:04d}" for i in range(200)]

# ── HELPERS ────────────────────────────────────────────────────────────
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def near(lat, lon, radius_deg=0.3):
    return (
        lat + random.uniform(-radius_deg, radius_deg),
        lon + random.uniform(-radius_deg, radius_deg),
    )

def far(lat, lon):
    """Return a location physically impossible to reach in < 5 min (geo-speed fraud)."""
    candidates = [(c[1], c[2]) for c in CITIES if haversine_km(lat, lon, c[1], c[2]) > 1000]
    if not candidates:
        candidates = [(CITIES[0][1], CITIES[0][2])]
    base = random.choice(candidates)
    return near(base[0], base[1], 0.1)

def make_transaction(user, fraud_type=None):
    merchant_id = random.choice(MERCHANTS)
    if random.random() < 0.005:
        legacy_id = f"merchant-legacy-{random.randint(1,50):04d}"
        merchant_id = legacy_id
    base_amount = user["avg_spend"]
    if fraud_type == "amount":
        amount = round(base_amount * random.uniform(8.0, 15.0), 2)
        lat, lon = near(user["home_lat"], user["home_lon"])
    elif fraud_type == "geo" and user["last_lat"] is not None:
        amount = round(base_amount * random.uniform(0.5, 1.5), 2)
        lat, lon = far(user["last_lat"], user["last_lon"])
    else:
        amount = round(base_amount * random.uniform(0.3, 2.0), 2)
        lat, lon = near(user["home_lat"], user["home_lon"])
    return {
        "user_id":      user["user_id"],
        "amount":       max(1.0, amount),
        "merchant_id":  merchant_id,
        "merchant_lat": round(lat, 6),
        "merchant_lon": round(lon, 6),
        "status":       "pending",
    }

def post_transaction(payload: dict) -> bool:
    try:
        r = requests.post(
            f"{BACKEND_URL}/api/transaction",
            json=payload,
            headers=HEADERS,
            timeout=10,
        )
        if r.status_code == 202:
            log.info('"event":"txn_queued","user_id":"%s","amount":%.2f',
                     payload["user_id"], payload["amount"])
            return True
        else:
            log.warning('"event":"txn_rejected","status":%d,"body":"%s"',
                        r.status_code, r.text[:200])
            return False
    except Exception as e:
        log.error('"event":"txn_error","error":"%s"', str(e))
        return False

def main():
    backoff = 1
    log.info('"event":"generator_start","rate_per_min":%.1f,"users":%d', TXN_RATE, len(USERS))
    while True:
        user = random.choice(USERS)
        r = random.random()
        if r < 0.04:
            fraud_type = "geo"
        elif r < 0.10:
            fraud_type = "amount"
        elif r < 0.13:
            fraud_type = "velocity"
        else:
            fraud_type = None
        if fraud_type == "velocity":
            for _ in range(6):
                payload = make_transaction(user)
                ok = post_transaction(payload)
                if not ok:
                    time.sleep(min(backoff, BACKOFF_MAX))
                    backoff = min(backoff * 2, BACKOFF_MAX)
                else:
                    backoff = 1
                time.sleep(random.uniform(0.5, 2.0))
        else:
            payload = make_transaction(user, fraud_type)
            ok = post_transaction(payload)
            if not ok:
                log.warning('"event":"backoff","seconds":%d', backoff)
                time.sleep(min(backoff, BACKOFF_MAX))
                backoff = min(backoff * 2, BACKOFF_MAX)
                continue
            else:
                backoff = 1
        user["last_lat"]  = payload.get("merchant_lat")
        user["last_lon"]  = payload.get("merchant_lon")
        user["last_time"] = time.time()
        sleep_s = random.expovariate(TXN_RATE / 60.0)
        time.sleep(sleep_s)

if __name__ == "__main__":
    main()
