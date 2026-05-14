"""
DAG 5: Daily Iceberg remove-orphan-files.
Runs remove_orphan_files on transactions_lake + processed_batches.
Complements fraud_iceberg_expire — expire removes expired-snapshot files,
this removes files referenced by no snapshot (failed commits, old append-mode
era). Heavy full-scan — daily, not hourly. Default older_than = now-3d
protects the active iceberg-writer's in-flight commit files.
"""
import json
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.models import Variable
from datetime import timedelta
from config import PIPELINE_EPOCH

default_args = {
    "owner": "fraud-lab",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Load Spark spec from Airflow Variable — no filesystem dependency at parse or execution time
_orphans_spec = json.loads(Variable.get("ORPHANS_ICEBERG_SPARK_SPEC"))
_orphans_spec["spec"]["image"] = Variable.get("FRAUD_SPARK_IMAGE")

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA MISS — DAG: {dag.dag_id} | Missed: {task_list} | Blocking: {blocking_task_list}")

with DAG(
    "fraud_iceberg_orphans",
    default_args=default_args,
    description="Daily Iceberg remove_orphan_files on fraud tables",
    # 04:00 UTC — 2h clear of the 02:00 customer/feature-refresh batch;
    # remove_orphan_files is a heavy full-scan, keep it off that window.
    schedule="0 4 * * *",
    start_date=PIPELINE_EPOCH,
    catchup=False,
    # max_active_runs=1 — remove_orphan_files is a destructive full-scan;
    # concurrent runs against the same tables conflict and waste resources.
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
    tags=["fraud", "iceberg", "orphans"],
) as dag:

    remove_orphans = SparkKubernetesOperator(
        task_id="remove_iceberg_orphan_files",
        namespace="bigdata",
        application_file=json.dumps(_orphans_spec),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        sla=timedelta(hours=1),
    )
