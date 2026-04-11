"""
DAG 2: Daily feature refresh at 02:00.
"""
import json
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.models import Variable
from datetime import timedelta
from kubernetes.client import models as k8s
from config import PIPELINE_EPOCH

default_args = {
    "owner": "fraud-lab",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SR_PASSWORD_ENV = k8s.V1EnvVar(
    name="SR_PASSWORD",
    value_from=k8s.V1EnvVarSource(
        secret_key_ref=k8s.V1SecretKeySelector(
            name="starrocks-credentials", key="root-password"
        )
    )
)

# Load Spark spec from Airflow Variable — no filesystem dependency at parse or execution time
_compact_spec = json.loads(Variable.get("COMPACT_ICEBERG_SPARK_SPEC"))
_compact_spec["spec"]["image"] = Variable.get("FRAUD_SPARK_IMAGE")
# Minimum risk profile row count — configurable without code change via Airflow Variable
_min_count = int(Variable.get("RISK_PROFILE_MIN_COUNT", default_var="45"))

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"SLA MISS — DAG: {dag.dag_id} | Missed: {task_list} | Blocking: {blocking_task_list}")

with DAG(
    "fraud_daily_feature_refresh",
    default_args=default_args,
    description="Daily risk profile refresh + Iceberg compaction",
    schedule="0 2 * * *",
    start_date=PIPELINE_EPOCH,
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["fraud", "features"],
) as dag:

    refresh_risk_profiles = KubernetesPodOperator(
        task_id="refresh_risk_profiles",
        namespace="bigdata",
        image="starrocks/fe-ubuntu:3.2.11",
        cmds=["bash", "-c"],
        arguments=[
            'set -euo pipefail; '
            'mysql -h starrocks-fe -P 9030 -u root --password="${SR_PASSWORD}" -e "'
            "INSERT OVERWRITE fraud.risk_profiles "
            "SELECT user_id, AVG(amount), STDDEV(amount), "
            "COUNT(*)/24.0, MAX(merchant_lat), MAX(merchant_lon), NOW() "
            "FROM fraud.transactions "
            "WHERE event_time >= DATE_SUB(NOW(), INTERVAL 30 DAY) "
            'GROUP BY user_id;"; '
            'echo "Risk profiles refreshed"'
        ],
        env_vars=[SR_PASSWORD_ENV],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"cpu": "1000m", "memory": "2Gi"},
        ),
        name="refresh-risk-profiles",
        is_delete_operator_pod=True,
        get_logs=True,
        sla=timedelta(hours=1),
    )

    verify_risk_profiles = KubernetesPodOperator(
        task_id="verify_risk_profiles",
        namespace="bigdata",
        image="starrocks/fe-ubuntu:3.2.11",
        cmds=["bash", "-c"],
        arguments=[
            'set -euo pipefail; '
            'COUNT=$(mysql -h starrocks-fe -P 9030 -u root '
            '--password="${SR_PASSWORD}" --skip-column-names '
            '-e "SELECT count(*) FROM fraud.risk_profiles;"); '
            'echo "Risk profiles: ${COUNT} users"; '
            f'if [ "${{COUNT}}" -lt "{_min_count}" ]; then '
            f'  echo "ALERT: Risk profile count ${{COUNT}} below minimum threshold ({_min_count})"; exit 1; '
            'fi; '
            'STALE=$(mysql -h starrocks-fe -P 9030 -u root '
            '--password="${SR_PASSWORD}" --skip-column-names '
            '-e "SELECT count(*) FROM fraud.risk_profiles WHERE DATE(updated_at) < CURDATE();"); '
            'echo "Stale profiles (not refreshed today): ${STALE}"; '
            'if [ "${STALE}" -gt "0" ]; then '
            '  echo "ALERT: ${STALE} profiles not updated today — partial write detected"; exit 1; '
            'fi; '
            'echo "Risk profiles verified: ${COUNT} users, all fresh"'
        ],
        env_vars=[SR_PASSWORD_ENV],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "256Mi"},
            limits={"cpu": "200m", "memory": "512Mi"},
        ),
        name="verify-risk-profiles",
        is_delete_operator_pod=True,
        get_logs=True,
        sla=timedelta(hours=1, minutes=15),
    )

    compact_iceberg = SparkKubernetesOperator(
        task_id="compact_iceberg_tables",
        namespace="bigdata",
        application_file=json.dumps(_compact_spec),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        sla=timedelta(hours=2),
    )

    refresh_risk_profiles >> verify_risk_profiles >> compact_iceberg
