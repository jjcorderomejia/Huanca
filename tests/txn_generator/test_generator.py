"""Transaction generator (k8s-apps/txn-generator/generator.py).

Covers the simulated user population, the three fraud patterns the pipeline is
meant to detect (amount anomaly, geo-speed violation, velocity burst), HTTP
posting, and the exponential backoff in the main loop.
"""
import importlib.util
import random
import re
from unittest.mock import MagicMock

import pytest
from huanca_testutils import GENERATOR_PATH

NY = (40.7128, -74.0060)
LA = (34.0522, -118.2437)


def _load(name="huanca_generator"):
    spec = importlib.util.spec_from_file_location(name, GENERATOR_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def gen(monkeypatch):
    monkeypatch.setenv("API_KEY", "k")
    monkeypatch.setenv("BACKEND_URL", "http://backend.test")
    module = _load()
    random.seed(1234)
    return module


class StopLoop(Exception):
    pass


# ── Geometry ───────────────────────────────────────────────────────────
def test_haversine_known_distance(gen):
    # NYC → LA great-circle distance is ~3936 km.
    assert gen.haversine_km(*NY, *LA) == pytest.approx(3936, rel=0.01)


def test_haversine_zero_and_symmetric(gen):
    assert gen.haversine_km(*NY, *NY) == pytest.approx(0.0, abs=1e-9)
    assert gen.haversine_km(*NY, *LA) == pytest.approx(gen.haversine_km(*LA, *NY))


def test_near_stays_within_radius(gen):
    for _ in range(500):
        lat, lon = gen.near(*NY, radius_deg=0.3)
        assert abs(lat - NY[0]) <= 0.3 and abs(lon - NY[1]) <= 0.3


def test_far_is_physically_unreachable(gen):
    # Every candidate base city is >1000 km away; ±0.1° jitter cannot close that gap.
    for city, lat, lon in gen.CITIES:
        for _ in range(50):
            assert gen.haversine_km(lat, lon, *gen.far(lat, lon)) > 950, city


# ── Simulated population ──────────────────────────────────────────────
def test_user_population_shape(gen):
    assert len(gen.USERS) == 50
    ids = [u["user_id"] for u in gen.USERS]
    assert len(set(ids)) == 50
    assert all(re.fullmatch(r"user_\d{3}", i) for i in ids)
    for i, user in enumerate(gen.USERS):
        _, city_lat, city_lon = gen.CITIES[i % len(gen.CITIES)]
        assert abs(user["home_lat"] - city_lat) <= 0.15
        assert abs(user["home_lon"] - city_lon) <= 0.15
        assert 20.0 <= user["avg_spend"] <= 300.0
        assert user["last_lat"] is None and user["last_time"] is None


def test_user_population_is_deterministic_across_restarts(monkeypatch):
    # Seeded population means a pod restart produces the same users and spend
    # profiles — the scorer's per-user baselines stay meaningful.
    a = _load("huanca_generator_a").USERS
    b = _load("huanca_generator_b").USERS
    assert a == b


def test_user_ids_match_customer_enrichment_csv(gen):
    from huanca_testutils import CUSTOMER_CSV_CONFIGMAP

    csv_ids = set(re.findall(r"^\s+(user_\d{3}),", CUSTOMER_CSV_CONFIGMAP.read_text(), flags=re.M))
    gen_ids = {u["user_id"] for u in gen.USERS}
    # Most generated users must have an enrichment row, otherwise z-score scoring
    # silently degrades to 0 for them (avg_amount_30d defaults to 0).
    assert len(gen_ids & csv_ids) >= 45


# ── Transactions ───────────────────────────────────────────────────────
def _user(gen, avg=100.0):
    return {**gen.USERS[0], "avg_spend": avg, "last_lat": None, "last_lon": None}


def test_transaction_matches_backend_contract(gen):
    txn = gen.make_transaction(_user(gen))
    assert set(txn) == {"user_id", "amount", "merchant_id", "merchant_lat", "merchant_lon", "status"}
    assert txn["status"] == "pending"
    assert txn["amount"] > 0
    assert txn["merchant_id"] in gen.MERCHANTS or txn["merchant_id"].startswith("merchant-legacy-")


def test_normal_amount_range(gen):
    user = _user(gen, avg=100.0)
    for _ in range(1000):
        txn = gen.make_transaction(user)
        assert 30.0 <= txn["amount"] <= 200.0
        assert gen.haversine_km(user["home_lat"], user["home_lon"], txn["merchant_lat"], txn["merchant_lon"]) < 60


def test_amount_floor_of_one(gen):
    user = _user(gen, avg=0.5)
    assert all(gen.make_transaction(user)["amount"] >= 1.0 for _ in range(200))


def test_amount_anomaly_is_8_to_15x_spend(gen):
    user = _user(gen, avg=100.0)
    for _ in range(500):
        amount = gen.make_transaction(user, "amount")["amount"]
        assert 800.0 <= amount <= 1500.0


def test_geo_fraud_jumps_far_from_last_location(gen):
    user = {**_user(gen), "last_lat": NY[0], "last_lon": NY[1]}
    for _ in range(200):
        txn = gen.make_transaction(user, "geo")
        assert gen.haversine_km(*NY, txn["merchant_lat"], txn["merchant_lon"]) > 950
        assert 50.0 <= txn["amount"] <= 150.0


def test_geo_fraud_without_history_falls_back_to_normal(gen):
    user = _user(gen)
    for _ in range(200):
        txn = gen.make_transaction(user, "geo")
        assert gen.haversine_km(user["home_lat"], user["home_lon"], txn["merchant_lat"], txn["merchant_lon"]) < 60


def test_legacy_merchant_ids_are_occasionally_emitted(gen, monkeypatch):
    monkeypatch.setattr(gen.random, "random", lambda: 0.001)
    txn = gen.make_transaction(_user(gen))
    assert re.fullmatch(r"merchant-legacy-\d{4}", txn["merchant_id"])


# ── HTTP ───────────────────────────────────────────────────────────────
def test_post_transaction_success(gen, monkeypatch):
    post = MagicMock(return_value=MagicMock(status_code=202))
    monkeypatch.setattr(gen.requests, "post", post)
    payload = gen.make_transaction(_user(gen))
    assert gen.post_transaction(payload) is True
    args, kwargs = post.call_args
    assert args[0] == "http://backend.test/api/transaction"
    assert kwargs["json"] == payload
    assert kwargs["headers"]["X-Api-Key"] == "k"
    assert kwargs["timeout"] == 10


@pytest.mark.parametrize("status", [200, 403, 422, 503])
def test_post_transaction_non_202_is_failure(gen, monkeypatch, status):
    monkeypatch.setattr(gen.requests, "post", MagicMock(return_value=MagicMock(status_code=status, text="err")))
    assert gen.post_transaction(gen.make_transaction(_user(gen))) is False


def test_post_transaction_network_error_is_failure(gen, monkeypatch):
    monkeypatch.setattr(gen.requests, "post", MagicMock(side_effect=ConnectionError("refused")))
    assert gen.post_transaction(gen.make_transaction(_user(gen))) is False


# ── Main loop ──────────────────────────────────────────────────────────
def _run_main_until(gen, monkeypatch, n_sleeps):
    sleeps = []

    def fake_sleep(s):
        sleeps.append(s)
        if len(sleeps) >= n_sleeps:
            raise StopLoop

    monkeypatch.setattr(gen.time, "sleep", fake_sleep)
    with pytest.raises(StopLoop):
        gen.main()
    return sleeps


def test_exponential_backoff_capped_at_max(gen, monkeypatch):
    monkeypatch.setattr(gen, "post_transaction", lambda payload: False)
    monkeypatch.setattr(gen.random, "random", lambda: 0.5)  # no fraud pattern
    sleeps = _run_main_until(gen, monkeypatch, 9)
    assert sleeps == [1, 2, 4, 8, 16, 32, 60, 60, 60]


def test_backoff_resets_after_success(gen, monkeypatch):
    results = iter([False, False, False, True, False])
    monkeypatch.setattr(gen, "post_transaction", lambda payload: next(results))
    monkeypatch.setattr(gen.random, "random", lambda: 0.5)
    monkeypatch.setattr(gen.random, "expovariate", lambda rate: 7.0)
    sleeps = _run_main_until(gen, monkeypatch, 5)
    # three backoffs, one Poisson inter-arrival sleep after success, then backoff restarts at 1
    assert sleeps == [1, 2, 4, 7.0, 1]


def test_velocity_burst_sends_six_for_same_user(gen, monkeypatch):
    posted = []
    monkeypatch.setattr(gen, "post_transaction", lambda payload: posted.append(payload) or True)
    monkeypatch.setattr(gen.random, "random", lambda: 0.11)  # velocity band [0.10, 0.13)
    _run_main_until(gen, monkeypatch, 7)  # 6 intra-burst sleeps + inter-arrival sleep
    assert len(posted) == 6
    assert len({p["user_id"] for p in posted}) == 1
    user = next(u for u in gen.USERS if u["user_id"] == posted[0]["user_id"])
    assert (user["last_lat"], user["last_lon"]) == (posted[-1]["merchant_lat"], posted[-1]["merchant_lon"])
