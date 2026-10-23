"""Cross-component schema contracts.

The pipeline is a chain of independently deployed pieces that agree on column
names only by convention: backend → transactions-raw → scorer →
transactions-scored → iceberg writer → Iceberg DDL, plus the StarRocks sinks.
A rename in one place silently breaks a downstream MERGE or sink write at
runtime. These tests read each side from source and fail on drift.
"""
import ast

import pytest
from huanca_testutils import (
    BACKEND_PATH,
    CUSTOMER_CSV_CONFIGMAP,
    CUSTOMER_LOADER_PATH,
    ICEBERG_SCHEMA_PATH,
    ICEBERG_WRITER_PATH,
    SCORER_PATH,
    STARROCKS_DDL_PATH,
    find_assign,
    parse,
    sql_create_columns,
    structfield_names,
)


def _function(path, name):
    return next(n for n in ast.walk(parse(path)) if isinstance(n, ast.FunctionDef) and n.name == name)


def _string_args(call):
    return [a.value for a in call.args if isinstance(a, ast.Constant) and isinstance(a.value, str)]


def _sink_blocks():
    """The innermost try-blocks of write_hot_sinks — one per sink write."""
    fn = _function(SCORER_PATH, "write_hot_sinks")
    trys = [n for n in ast.walk(fn) if isinstance(n, ast.Try)]
    return [t for t in trys if not any(isinstance(c, ast.Try) for b in t.body for c in ast.walk(b))]


def _struct_payload_for_topic(topic_var):
    """String field list of the to_json(struct(...)) written to a given Kafka topic in write_hot_sinks."""
    for node in _sink_blocks():
        src = ast.unparse(node)
        if f'"topic", {topic_var}' in src or f"'topic', {topic_var}" in src:
            for call in ast.walk(node):
                if isinstance(call, ast.Call) and getattr(call.func, "id", None) == "struct":
                    return _string_args(call)
    raise LookupError(topic_var)


def _starrocks_sink_columns(table):
    """Output column names of the StarRocks write targeting ``{SR_DB}.<table>``."""
    for node in _sink_blocks():
        if f"{{SR_DB}}.{table}" in ast.unparse(node):
            for call in ast.walk(node):
                if isinstance(call, ast.Call) and getattr(call.func, "attr", None) == "select":
                    cols = []
                    for arg in call.args:
                        if isinstance(arg, ast.Constant):
                            cols.append(arg.value)
                        else:  # col("x").alias("y") / lit(None).cast(...).alias("y")
                            cols.append(arg.args[0].value)
                    return cols
    raise LookupError(table)


def _iceberg_ddl(table):
    sql = "\n".join(
        n.value for n in ast.walk(parse(ICEBERG_SCHEMA_PATH)) if isinstance(n, ast.Constant) and isinstance(n.value, str)
    )
    return sql_create_columns(sql, f"iceberg.fraud.{table}")


def _starrocks_ddl(table):
    return sql_create_columns(STARROCKS_DDL_PATH.read_text(), table)


# ── Ingest: backend payload → scorer input schema ─────────────────────
def test_backend_payload_matches_scorer_input_schema():
    payload = find_assign(BACKEND_PATH, "payload", inside_function="create_transaction")
    keys = [k.value for k in payload.value.keys]
    assert keys == structfield_names(SCORER_PATH, "txn_schema")


# ── transactions-scored topic → iceberg writer → Iceberg DDL ───────────
def test_scored_topic_payload_matches_iceberg_writer_schema():
    assert _struct_payload_for_topic("TOPIC_SCORED") == structfield_names(ICEBERG_WRITER_PATH, "scored_schema")


def test_iceberg_writer_schema_matches_transactions_lake_ddl():
    # MERGE ... WHEN NOT MATCHED THEN INSERT * resolves columns by name.
    assert structfield_names(ICEBERG_WRITER_PATH, "scored_schema") == _iceberg_ddl("transactions_lake")


def test_merge_key_columns_present_in_lake():
    assert {"k_topic", "k_partition", "k_offset"} <= set(_iceberg_ddl("transactions_lake"))


# ── Velocity state output schema ──────────────────────────────────────
def test_velocity_output_schema_covers_state_fn_columns():
    out_cols = find_assign(SCORER_PATH, "_out_cols", inside_function="_velocity_state_fn")
    emitted = {e.value for e in out_cols.value.elts} | {"velocity_5min"}
    assert emitted == set(structfield_names(SCORER_PATH, "_VELOCITY_OUTPUT_SCHEMA"))


# ── StarRocks sinks ↔ StarRocks DDL ───────────────────────────────────
@pytest.mark.parametrize("table", ["fraud_scores", "risk_profiles"])
def test_starrocks_sink_columns_match_ddl(table):
    assert _starrocks_sink_columns(table) == _starrocks_ddl(table)


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Known drift: scorer writes geo_speed_kmh to fraud.transactions but the "
        "StarRocks DDL in gitops/bigdata/starrocks/init-ddl-job.yaml declares "
        "geo_distance_km. Remove this marker once the DDL and sink agree."
    ),
)
def test_starrocks_transactions_sink_columns_exist_in_ddl():
    assert set(_starrocks_sink_columns("transactions")) <= set(_starrocks_ddl("transactions"))


def test_risk_profiles_iceberg_ddl_matches_starrocks_ddl():
    # risk_profiles_to_iceberg.py copies the StarRocks table into Iceberg verbatim,
    # and compute_geo reads last_merchant_lat/lon + updated_at from the Iceberg copy.
    iceberg = _iceberg_ddl("risk_profiles")
    assert iceberg == _starrocks_ddl("risk_profiles")
    assert {"last_merchant_lat", "last_merchant_lon", "updated_at"} <= set(iceberg)


# ── Customer enrichment ───────────────────────────────────────────────
def test_customer_csv_loader_matches_ddl_and_configmap():
    ddl = _iceberg_ddl("customers")
    loader = find_assign(CUSTOMER_LOADER_PATH, "customers")
    selected = [
        c.args[0].value
        for c in ast.walk(loader.value)
        if isinstance(c, ast.Call) and getattr(c.func, "id", None) == "col"
    ]
    header = next(
        line.strip() for line in CUSTOMER_CSV_CONFIGMAP.read_text().splitlines() if line.strip().startswith("user_id,")
    )
    assert selected == ddl
    assert header.split(",") == ddl
