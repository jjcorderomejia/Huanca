"""
DAG 3: Daily customer CSV to Iceberg refresh at 02:00.
Loads customer enrichment data from customer-csv ConfigMap into fraud.customers Iceberg table.
SparkKubernetesOperator exit status is the verification — Spark fails fast on write errors.
"""
import yaml
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.models import Variable
from datetime import timedelta
from config import PIPELINE_EPOCH

default_args = {
    "owner": "fraud-lab",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

SPARK_IMAGE = Variable.get("FRAUD_SPARK_IMAGE")  # Set in Phase 6.2 — no default, fails fast if missing

# Load SparkApplication manifest from ConfigMap-mounted path — single source of truth
with open("/opt/spark-manifests/customer_csv_to_iceberg_spark_app.yaml") as _f:
    _customer_spec = yaml.safe_load(_f)
_customer_spec["spec"]["image"] = SPARK_IMAGE

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA MISS — DAG: {dag.dag_id} | Missed: {task_list} | Blocking: {blocking_task_list}")

with DAG(
    "fraud_daily_customer_refresh",
    default_args=default_args,
    description="Daily customer CSV to Iceberg refresh",
    schedule="0 2 * * *",
    start_date=PIPELINE_EPOCH,
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["fraud", "customers"],
) as dag:

    load_customers = SparkKubernetesOperator(
        task_id="load_customer_csv_to_iceberg",
        namespace="bigdata",
        application_file=_customer_spec,
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        sla=timedelta(hours=1),
    )
