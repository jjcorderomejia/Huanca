"""Pure-Python scoring logic from spark-jobs/fraud_stream_to_starrocks.py.

The streaming job builds a SparkSession and starts queries at import time, so
the functions under test are compiled directly from the job's source (see
``huanca_testutils.load_symbols``) — no Spark cluster or pyspark needed.
"""
from datetime import datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest
from huanca_testutils import SCORER_PATH, load_symbols

NY = (40.7128, -74.0060)
LA = (34.0522, -118.2437)


@pytest.fixture
def scorer():
    return load_symbols(
        SCORER_PATH,
        ["haversine", "compute_geo", "_velocity_state_fn", "VELOCITY_WINDOW_SEC", "VELOCITY_STATE_TIMEOUT"],
        namespace={"risk_bc": SimpleNamespace(value={})},
    )


# ── haversine ──────────────────────────────────────────────────────────
def test_haversine_ny_to_la(scorer):
    assert scorer["haversine"](*NY, *LA) == pytest.approx(3936, rel=0.01)


def test_haversine_zero_distance(scorer):
    assert scorer["haversine"](*NY, *NY) == pytest.approx(0.0, abs=1e-9)


@pytest.mark.parametrize("missing", range(4))
def test_haversine_missing_coordinate_is_zero(scorer, missing):
    args = [*NY, *LA]
    args[missing] = None
    assert scorer["haversine"](*args) == 0.0


# ── compute_geo (km/h since last known location) ──────────────────────
def _with_profile(scorer, profile):
    scorer["risk_bc"] = SimpleNamespace(value={"u1": profile})
    return scorer["compute_geo"]


def test_geo_speed_ny_to_la_in_one_hour(scorer):
    t0 = datetime(2026, 1, 1, 12, 0, 0)
    geo = _with_profile(scorer, {"last_merchant_lat": NY[0], "last_merchant_lon": NY[1], "updated_at": t0})
    speed = geo("u1", *LA, t0 + timedelta(hours=1))
    assert speed == pytest.approx(3936, rel=0.01)
    assert speed > 500  # above the default GEO_SPEED_THRESHOLD


def test_geo_speed_plausible_local_move(scorer):
    t0 = datetime(2026, 1, 1, 12, 0, 0)
    geo = _with_profile(scorer, {"last_merchant_lat": NY[0], "last_merchant_lon": NY[1], "updated_at": t0})
    # ~11 km in 30 minutes ≈ 22 km/h
    assert geo("u1", NY[0] + 0.1, NY[1], t0 + timedelta(minutes=30)) == pytest.approx(22.2, rel=0.05)


def test_geo_unknown_user_is_zero(scorer):
    geo = _with_profile(scorer, {})
    assert geo("someone-else", *LA, datetime(2026, 1, 1)) == 0.0


def test_geo_profile_without_location_is_zero(scorer):
    geo = _with_profile(scorer, {"last_merchant_lat": None, "last_merchant_lon": None, "updated_at": datetime(2026, 1, 1)})
    assert geo("u1", *LA, datetime(2026, 1, 2)) == 0.0


@pytest.mark.parametrize("delta", [timedelta(0), timedelta(hours=-1)])
def test_geo_non_positive_elapsed_time_is_zero(scorer, delta):
    # Out-of-order events must not produce infinite / negative speeds.
    t0 = datetime(2026, 1, 1, 12)
    geo = _with_profile(scorer, {"last_merchant_lat": NY[0], "last_merchant_lon": NY[1], "updated_at": t0})
    assert geo("u1", *LA, t0 + delta) == 0.0


def test_geo_missing_timestamps_is_zero(scorer):
    geo = _with_profile(scorer, {"last_merchant_lat": NY[0], "last_merchant_lon": NY[1], "updated_at": None})
    assert geo("u1", *LA, datetime(2026, 1, 1)) == 0.0
    geo = _with_profile(scorer, {"last_merchant_lat": NY[0], "last_merchant_lon": NY[1], "updated_at": datetime(2026, 1, 1)})
    assert geo("u1", *LA, None) == 0.0


# ── velocity state function (applyInPandasWithState) ──────────────────
class FakeGroupState:
    def __init__(self, timestamps=None, timed_out=False):
        self._ts = timestamps
        self.hasTimedOut = timed_out
        self.removed = False
        self.updated = None
        self.timeout_ms = None

    @property
    def exists(self):
        return self._ts is not None

    @property
    def get(self):
        return (self._ts,)

    def update(self, value):
        self.updated = value

    def remove(self):
        self.removed = True

    def setTimeoutDuration(self, ms):
        self.timeout_ms = ms


def _batch(times, user="u1"):
    n = len(times)
    return pd.DataFrame(
        {
            "transaction_id": [f"t{i}" for i in range(n)],
            "user_id": [user] * n,
            "amount": [10.0] * n,
            "merchant_id": ["m"] * n,
            "merchant_lat": [0.0] * n,
            "merchant_lon": [0.0] * n,
            "status": ["pending"] * n,
            "event_time": pd.to_datetime(times),
            "ingest_time": pd.to_datetime(times),
            "raw_json": ["{}"] * n,
            "k_topic": ["transactions-raw"] * n,
            "k_partition": [0] * n,
            "k_offset": list(range(n)),
        }
    )


def _run(scorer, state, *pdfs):
    out = list(scorer["_velocity_state_fn"](("u1",), iter(pdfs), state))
    return pd.concat(out) if out else None


def test_velocity_counts_events_in_first_batch(scorer):
    state = FakeGroupState()
    out = _run(scorer, state, _batch(["2026-01-01 12:00:00", "2026-01-01 12:01:00", "2026-01-01 12:02:00"]))
    assert list(out["velocity_5min"]) == [3, 3, 3]
    assert len(state.updated[0]) == 3
    assert state.timeout_ms == scorer["VELOCITY_STATE_TIMEOUT"] == 10 * 60 * 1000


def test_velocity_includes_prior_state_within_window(scorer):
    prior = [pd.Timestamp("2026-01-01 11:58:00").timestamp(), pd.Timestamp("2026-01-01 11:59:00").timestamp()]
    state = FakeGroupState(prior)
    out = _run(scorer, state, _batch(["2026-01-01 12:00:00"]))
    assert list(out["velocity_5min"]) == [3]


def test_velocity_evicts_events_older_than_five_minutes(scorer):
    stale = pd.Timestamp("2026-01-01 11:50:00").timestamp()
    edge = pd.Timestamp("2026-01-01 11:55:00").timestamp()  # exactly 5 min before → kept (>= cutoff)
    state = FakeGroupState([stale, edge])
    out = _run(scorer, state, _batch(["2026-01-01 12:00:00"]))
    assert list(out["velocity_5min"]) == [2]
    assert stale not in state.updated[0]
    assert edge in state.updated[0]


def test_velocity_window_anchored_to_event_time_not_wall_clock(scorer):
    # Late-arriving data from years ago must still be counted together.
    out = _run(scorer, FakeGroupState(), _batch(["2019-05-01 08:00:00"] * 6))
    assert list(out["velocity_5min"]) == [6] * 6


def test_velocity_spans_multiple_pandas_chunks(scorer):
    out = _run(scorer, FakeGroupState(), _batch(["2026-01-01 12:00:00"] * 2), _batch(["2026-01-01 12:01:00"] * 4))
    assert len(out) == 6
    assert set(out["velocity_5min"]) == {6}


def test_velocity_ignores_null_event_times(scorer):
    pdf = _batch(["2026-01-01 12:00:00", "2026-01-01 12:00:30"])
    pdf.loc[1, "event_time"] = pd.NaT
    out = _run(scorer, FakeGroupState(), pdf)
    assert list(out["velocity_5min"]) == [1, 1]


def test_velocity_timeout_removes_state_and_emits_nothing(scorer):
    state = FakeGroupState([1.0], timed_out=True)
    assert _run(scorer, state) is None
    assert state.removed is True
    assert state.updated is None


def test_velocity_output_columns(scorer):
    out = _run(scorer, FakeGroupState(), _batch(["2026-01-01 12:00:00"]))
    assert list(out.columns) == [
        "transaction_id", "user_id", "amount", "merchant_id",
        "merchant_lat", "merchant_lon", "status",
        "event_time", "ingest_time",
        "raw_json", "k_topic", "k_partition", "k_offset",
        "velocity_5min",
    ]
