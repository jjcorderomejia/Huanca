"""
Shared pipeline configuration — imported by all fraud DAGs.
PIPELINE_EPOCH: production go-live date, defines the scheduling epoch.
All DAGs use catchup=False — this date anchors audit trails and incident reconstruction.
"""
import pendulum

PIPELINE_EPOCH = pendulum.datetime(2026, 3, 24, tz="UTC")
