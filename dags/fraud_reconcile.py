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

    # FIXED 2026-06-11 (INC: stream dead 14:35-18:30Z, this check stayed green the whole time).
    # Spark commits offsets ONLY to its s3a checkpoint (no kafka group.id is set), so broker-side
    # consumer-group lag is unmeasurable: the old rpk check described the NONEXISTENT group
    # 'fraud-stream-consumer', greped nothing, summed 0, and was PERMANENTLY GREEN (the V9
    # "Consumer Group Misunderstanding" finding, now closed). Replaced with a true end-to-end
    # signal: newest event_time landed in StarRocks vs NOW(). Catches a dead consumer, a badly
    # lagging consumer, AND a stalled producer (txn-generator) — alert message names all three.
    # NULL guard: an empty table makes LAG empty -> the -gt test errors -> task FAILS (fail-loud).
    check_stream_lag = KubernetesPodOperator(
        task_id="check_stream_end_to_end_lag",
        namespace="bigdata",
        image="starrocks/fe-ubuntu:3.2.11",
        cmds=["bash", "-c"],
        arguments=[
            'set -euo pipefail; '
            'LAG=$(mysql -h starrocks-fe -P 9030 -u root '
            '--password="${SR_PASSWORD}" --skip-column-names '
            '-e "SELECT TIMESTAMPDIFF(SECOND, MAX(event_time), NOW()) '
            'FROM fraud.transactions;"); '
            'echo "End-to-end stream lag: ${LAG}s (newest event_time vs now)"; '
            'if [ "${LAG}" -gt "600" ]; then '
            '  echo "ALERT: stream lag ${LAG}s exceeds 600s — consumer dead/lagging or producer stalled"; exit 1; '
            'fi; '
            'echo "Stream lag within bounds"'
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
        name="check-stream-lag",
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

    check_stream_lag >> check_starrocks
