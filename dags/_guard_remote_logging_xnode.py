"""
THROWAWAY GUARD DAG — airflow-logs PVC removal (§3-2026-06-07 / P11-4).

Certifies that with remote logging to MinIO (and the airflow-logs RWO PVC gone),
a KubernetesExecutor task pod forced onto hetz3 (cross-node from the node1
api-server/scheduler) BOTH:
  (1) streams its live logs while RUNNING (api-server -> the task pod's own
      worker_log_server_port 8793, cross-node), and
  (2) ships the final log to s3://airflow/logs on completion.

This is a plain TaskFlow @task: the code runs INSIDE the KE task pod itself (the
`base` container), so the running task pod is the log producer and the UI/
api-server must fetch its live logs over :8793 cross-node — exactly the path the
GUARD requires. executor_config pod_override pins the KE pod to hetz3.

The task sleeps ~120s emitting timestamped heartbeats so the live-stream window
is observable. DELETE THIS DAG FILE once the GUARD is certified.
"""
import logging
import socket
import time
from datetime import datetime

from airflow.decorators import dag, task
from kubernetes.client import models as k8s

log = logging.getLogger("airflow.task")


@dag(
    dag_id="_guard_remote_logging_xnode",
    description="THROWAWAY: certify cross-node live-log stream + S3 log ship",
    schedule=None,            # manual trigger only
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["guard", "throwaway", "remote-logging"],
)
def guard_dag():
    @task(
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    # force the KE task pod cross-node onto hetz3
                    node_selector={"kubernetes.io/hostname": "hetz3"},
                    containers=[k8s.V1Container(name="base")],
                )
            )
        }
    )
    def xnode_heartbeat():
        host = socket.gethostname()
        log.info("GUARD START host=%s ip(s) follow", host)
        for i in range(24):
            log.info(
                "GUARD heartbeat %02d host=%s at %s",
                i, host, datetime.utcnow().strftime("%H:%M:%SZ"),
            )
            time.sleep(5)
        log.info("GUARD-COMPLETE host=%s", host)

    xnode_heartbeat()


guard_dag()
