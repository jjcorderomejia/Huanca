"""
DAG 4: Hourly Iceberg expire-snapshots.
Runs expire_snapshots(retain_last=1) on transactions_lake + processed_batches.
Independent of compact_iceberg — pure expire, no rewrite.
Recovery comes from Spark checkpoint, idempotency from MERGE on Kafka offsets,
so old snapshots have zero functional value (retain_last=1 is correct by design).
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
_expire_spec = json.loads(Variable.get("EXPIRE_ICEBERG_SPARK_SPEC"))
_expire_spec["spec"]["image"] = Variable.get("FRAUD_SPARK_IMAGE")

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA MISS — DAG: {dag.dag_id} | Missed: {task_list} | Blocking: {blocking_task_list}")

with DAG(
    "fraud_iceberg_expire",
    default_args=default_args,
    description="Hourly Iceberg expire_snapshots(retain_last=1) on fraud tables",
    schedule="@hourly",
    start_date=PIPELINE_EPOCH,
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["fraud", "iceberg", "expire"],
) as dag:

    expire_snapshots = SparkKubernetesOperator(
        task_id="expire_iceberg_snapshots",
        namespace="bigdata",
        application_file=json.dumps(_expire_spec),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        sla=timedelta(minutes=30),
    )
