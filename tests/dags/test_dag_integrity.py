"""Airflow DAG integrity (dags/).

Parses the DAG folder with a real Airflow DagBag — the same code path the
scheduler uses — with Airflow Variables supplied via AIRFLOW_VAR_* env vars,
so no metadata database or cluster is needed. Skipped when Airflow is not
installed (CI installs it in a dedicated job).
"""
import json
import sys

import pytest
from huanca_testutils import DAGS_DIR

pytest.importorskip("airflow")

SPARK_SPEC_VARS = [
    "CUSTOMER_CSV_SPARK_SPEC",
    "COMPACT_ICEBERG_SPARK_SPEC",
    "RISK_SYNC_ICEBERG_SPARK_SPEC",
    "EXPIRE_ICEBERG_SPARK_SPEC",
    "ORPHANS_ICEBERG_SPARK_SPEC",
]
SPARK_IMAGE = "ghcr.io/example/spark-fraud-job:test"
MIN_COUNT = "45"

# Timetable summaries: Airflow normalises "@hourly" to its cron form.
EXPECTED = {
    "fraud_hourly_reconcile": ("0 * * * *", ["check_stream_end_to_end_lag", "check_starrocks_count"]),
    "fraud_daily_feature_refresh": (
        "0 2 * * *",
        ["refresh_risk_profiles", "verify_risk_profiles", "sync_risk_profiles_iceberg", "compact_iceberg_tables"],
    ),
    "fraud_daily_customer_refresh": ("0 2 * * *", ["load_customer_csv_to_iceberg"]),
    "fraud_iceberg_expire": ("0 * * * *", ["expire_iceberg_snapshots"]),
    "fraud_iceberg_orphans": ("0 4 * * *", ["remove_iceberg_orphan_files"]),
}


@pytest.fixture(scope="module")
def dagbag(tmp_path_factory):
    mp = pytest.MonkeyPatch()
    home = tmp_path_factory.mktemp("airflow_home")
    mp.setenv("AIRFLOW_HOME", str(home))
    mp.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
    mp.setenv("AIRFLOW__CORE__UNIT_TEST_MODE", "true")
    for var in SPARK_SPEC_VARS:
        spec = {"apiVersion": "sparkoperator.k8s.io/v1beta2", "kind": "SparkApplication",
                "metadata": {"name": var.lower()}, "spec": {"image": "placeholder"}}
        mp.setenv(f"AIRFLOW_VAR_{var}", json.dumps(spec))
    mp.setenv("AIRFLOW_VAR_FRAUD_SPARK_IMAGE", SPARK_IMAGE)
    mp.setenv("AIRFLOW_VAR_RISK_PROFILE_MIN_COUNT", MIN_COUNT)
    # In the cluster, git-sync puts dags/ on sys.path so `from config import ...` resolves.
    mp.syspath_prepend(str(DAGS_DIR))

    from airflow.models.dagbag import DagBag

    bag = DagBag(dag_folder=str(DAGS_DIR), include_examples=False)
    yield bag
    mp.undo()
    sys.modules.pop("config", None)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_present(dagbag):
    assert set(dagbag.dag_ids) == set(EXPECTED)


@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_schedule_and_task_chain(dagbag, dag_id):
    schedule, chain = EXPECTED[dag_id]
    dag = dagbag.dags[dag_id]
    assert dag.timetable.summary == schedule
    assert sorted(dag.task_ids) == sorted(chain)
    for upstream, downstream in zip(chain, chain[1:]):
        assert dag.get_task(upstream).downstream_task_ids == {downstream}
    assert not dag.get_task(chain[-1]).downstream_task_ids


@pytest.mark.parametrize("dag_id", sorted(EXPECTED))
def test_common_dag_policy(dagbag, dag_id):
    from config import PIPELINE_EPOCH

    dag = dagbag.dags[dag_id]
    assert dag.catchup is False
    assert dag.start_date == PIPELINE_EPOCH
    assert "fraud" in dag.tags
    for task in dag.tasks:
        assert task.owner == "fraud-lab"
        assert task.retries >= 1


@pytest.mark.parametrize("dag_id", ["fraud_iceberg_expire", "fraud_iceberg_orphans"])
def test_destructive_maintenance_never_overlaps(dagbag, dag_id):
    assert dagbag.dags[dag_id].max_active_runs == 1


def test_spark_tasks_use_injected_image(dagbag):
    spark_tasks = [
        t for d in dagbag.dags.values() for t in d.tasks if type(t).__name__ == "SparkKubernetesOperator"
    ]
    assert len(spark_tasks) == 5
    for task in spark_tasks:
        spec = json.loads(task.application_file)
        assert spec["kind"] == "SparkApplication"
        assert spec["spec"]["image"] == SPARK_IMAGE
        assert task.namespace == "bigdata"


def test_risk_profile_threshold_comes_from_variable(dagbag):
    task = dagbag.dags["fraud_daily_feature_refresh"].get_task("verify_risk_profiles")
    script = " ".join(task.arguments)
    assert f'-lt "{MIN_COUNT}"' in script
    assert "set -euo pipefail" in script


def test_reconcile_lag_check_fails_loud(dagbag):
    task = dagbag.dags["fraud_hourly_reconcile"].get_task("check_stream_end_to_end_lag")
    script = " ".join(task.arguments)
    assert "set -euo pipefail" in script
    assert "MAX(event_time)" in script
    assert "exit 1" in script


def test_secrets_come_from_k8s_secret_refs(dagbag):
    for dag_id in ("fraud_hourly_reconcile", "fraud_daily_feature_refresh"):
        for task in dagbag.dags[dag_id].tasks:
            if type(task).__name__ != "KubernetesPodOperator":
                continue
            env = {e.name: e for e in task.env_vars}
            sr = env["SR_PASSWORD"]
            assert sr.value is None
            assert sr.value_from.secret_key_ref.name == "starrocks-credentials"
