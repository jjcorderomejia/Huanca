"""
DAG 1: Hourly reconciliation — checks Redpanda consumer lag and StarRocks row count.
Alerts on lag exceeding threshold or zero rows in last hour.
"""
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from datetime import timedelta
from kubernetes.client import models as k8s
from config import PIPELINE_EPOCH

default_args = {
    "owner": "fraud-lab",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA MISS — DAG: {dag.dag_id} | Missed: {task_list} | Blocking: {blocking_task_list}")

with DAG(
    "fraud_hourly_reconcile",
    default_args=default_args,
    description="Hourly pipeline reconciliation",
    schedule="@hourly",
    start_date=PIPELINE_EPOCH,
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["fraud", "reconcile"],
) as dag:

    check_redpanda = KubernetesPodOperator(
        task_id="check_redpanda_consumer_lag",
        namespace="bigdata",
        image="docker.redpanda.com/redpandadata/redpanda:v24.1.7",
        cmds=["bash", "-c"],
        arguments=[
            'set -euo pipefail; '
            'LAG=$(rpk group describe fraud-stream-consumer '
            '--brokers fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9093 '
            '| grep transactions-raw | awk \'{sum+=$NF} END {print sum+0}\'); '
            'echo "Consumer lag: ${LAG}"; '
            'if [ "${LAG}" -gt "10000" ]; then '
            '  echo "ALERT: Consumer lag ${LAG} exceeds threshold (10000)"; exit 1; '
            'fi; '
            'echo "Consumer lag within bounds"'
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "200m", "memory": "512Mi"},
        ),
        name="check-redpanda",
        is_delete_operator_pod=True,
        get_logs=True,
        sla=timedelta(minutes=30),
    )

    check_starrocks = KubernetesPodOperator(
        task_id="check_starrocks_count",
        namespace="bigdata",
        image="starrocks/fe-ubuntu:3.2.11",
        cmds=["bash", "-c"],
        arguments=[
            'set -euo pipefail; '
            'COUNT=$(mysql -h starrocks-fe -P 9030 -u root '
            '--password="${SR_PASSWORD}" --skip-column-names '
            '-e "SELECT count(*) FROM fraud.transactions '
            'WHERE ingest_time >= NOW() - INTERVAL 1 HOUR;"); '
            'echo "Transactions in last hour: ${COUNT}"; '
            'if [ "${COUNT}" -eq "0" ]; then '
            '  echo "ALERT: Zero transactions in last hour"; exit 1; '
            'fi; '
            'echo "Pipeline reconciliation passed"'
        ],
        env_vars=[
            k8s.V1EnvVar(
                name="SR_PASSWORD",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="starrocks-credentials",
                        key="root-password"
                    )
                )
            )
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "200m", "memory": "512Mi"},
        ),
        name="check-starrocks",
        is_delete_operator_pod=True,
        get_logs=True,
        sla=timedelta(minutes=45),
    )

    check_redpanda >> check_starrocks
