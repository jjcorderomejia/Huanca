# MinIO — `bigdata` object store

Single-node MinIO that backs the Huanca fraud lab: Iceberg warehouse, Spark
Structured-Streaming checkpoints + Spark History event logs, Airflow remote
task logs, and Terraform state.

## What it is

- **Namespace:** `bigdata`
- **Workload:** StatefulSet [`minio`](./minio-statefulset.yaml), `replicas: 1`
  (single pod). Pod placement **floats** across nodes on restart — no node pin
  (a `topologySpreadConstraints` exists but is `ScheduleAnyway`, a no-op at 1 replica).
- **Image (pinned):** `quay.io/minio/minio:RELEASE.2024-05-01T01-11-10Z` (~13 months old).
- **Endpoint (in-cluster):** `http://minio.bigdata.svc.cluster.local:9000`
  — **path-style, no TLS**. Console on `:9001`.
- **Credentials:** a single root credential (`minio-secret`). `mc admin user ls`
  is **empty** — every consumer uses root.

## Topology & durability — READ THIS

> **This is the cluster's most concentrated data-loss SPOF. Read before
> touching anything.**

- **`xl-single`: parity-0, single drive, NO erasure redundancy.**
  `mc admin info` reports Pool 1 / Erasure sets 1 / Drives per erasure set 1.
  There is no intra-MinIO redundancy — a single corrupt/lost drive is data loss.
- **Storage:** PVC [`data-minio-0`] = **50Gi** on `longhorn-r2`. Longhorn keeps a
  block-level replica (RF2) so the **volume** survives a node loss — but a Longhorn
  replica is **NOT a backup** (it faithfully replicates deletes/corruption too).
- **NO continuous / off-host backup of any kind.** No Longhorn backuptarget
  (URL empty), no recurringjob, no Velero, no `mc mirror`.
- **~13-month-old image**, a **single shared root credential**, **no per-bucket
  quotas**, and the StatefulSet declares **no resource limits** (`resources: {}`).
- **DR posture: data loss is recovered by RE-DERIVING, not restoring.**
  - Iceberg warehouse → re-ingest.
  - Spark SS checkpoints → Kafka/Redpanda offset replay (`startingOffsets=earliest`).
  - There is no snapshot to roll back to.

## Buckets (4) + the `spark-events/` prefix

There are exactly **4 buckets**. `spark-events/` is a **prefix inside
`checkpoints`**, not a bucket.

| Bucket / prefix | Writer / consumer | Versioning | ILM |
|---|---|---|---|
| `checkpoints` | Spark Structured-Streaming checkpoints | **Suspended** | whole-bucket `ExpiredObjectDeleteMarker` (see below) |
| `checkpoints/spark-events/` | Spark History Server event logs | (inherits) | `spark-events/` prefix Expire **21d** (see below) |
| `iceberg` | Iceberg warehouse (Spark writers) | — | — |
| `airflow` | Airflow remote task logs (`s3://airflow/logs`) | — | — |
| `tf-state` | Terraform remote state | — | — |

`checkpoints` and `iceberg` are created by the bootstrap Job
([`minio-buckets-job.yaml`](./minio-buckets-job.yaml)). **`airflow` and
`tf-state` are created out-of-band — the bootstrap Job does NOT create them**
(drift to reconcile; see hardening checklist).

## ILM (`checkpoints`, TWO rules)

1. **`spark-events/` prefix — Expire 21 days.** Raised from the **7d** bootstrap
   default during the **2026-06-17** PVC-expansion / Spark-History-Server fix
   incident, to keep ~3 weeks of Spark History. Safe while usage ≪ 50Gi;
   **revisit if `spark-events/` approaches ~20GiB.**
2. **Whole-bucket `ExpiredObjectDeleteMarker`** — the permanent **S6** sweep that
   reaps expired delete markers under Suspended versioning. **DO NOT remove**
   (rule id `d8arn10bue0s76ksubj0`); see
   [`S6-minio-checkpoint-version-bloat.md`](../../../../runbooks/huanca/ops/S6-minio-checkpoint-version-bloat.md).

`checkpoints` versioning is **Suspended** (intentional — versioning without
noncurrent expiration previously produced ~11G of noncurrent + delete-marker bloat).

## Storage / expansion

- PVC `data-minio-0` is **50Gi** (expanded **online 5→50Gi on 2026-06-17**;
  Longhorn + ext4 auto-resize, **no pod restart** — see the cluster-config runlog
  below). At ~10% used.
- **Future resizes:** `kubectl patch pvc data-minio-0 -n bigdata` (online).
- **The StatefulSet `volumeClaimTemplates` is immutable.** This manifest's
  template says `50Gi` to match live intent, but the live PVC was expanded
  in-place; **fully syncing the StatefulSet object to 50Gi requires a
  `--cascade=orphan` recreate, which is DEFERRED.** Gate any such recreate behind
  a Longhorn snapshot **+ §3 cert + a real backup target** — never recreate this
  no-backup store casually.

## "Before Condor onboards" — hardening checklist

- [ ] **Durable off-host backup** — Longhorn backuptarget + recurringjob, and/or
      `mc mirror` of at least `checkpoints` + `iceberg` to off-host storage.
- [ ] **Per-tenant credentials/policies** — stop sharing the root credential.
- [ ] **Per-bucket quotas** — currently unlimited (another 507/full-disk risk).
- [ ] **StatefulSet resource limits + `GOMEMLIMIT`** — an OOM of the single pod is
      a **cluster-wide S3 outage** (separate §3 cert).
- [ ] **Reconcile the bootstrap Job** — it does not create `airflow`, `tf-state`,
      or `condor-models`.
- [ ] **Revisit parity** — `xl-single` parity-0 has no redundancy; real parity
      needs a multi-node MinIO topology.

## See also

- [`runbooks/huanca/ops/S5-minio-iceberg-orphan-cleanup.md`](../../../../runbooks/huanca/ops/S5-minio-iceberg-orphan-cleanup.md)
- [`runbooks/huanca/ops/S6-minio-checkpoint-version-bloat.md`](../../../../runbooks/huanca/ops/S6-minio-checkpoint-version-bloat.md)
- [`cluster-config/docs/STORAGE_INVENTORY.md`](../../../../cluster-config/docs/STORAGE_INVENTORY.md) — `data-minio-0` row
- [`./minio-statefulset.yaml`](./minio-statefulset.yaml) · [`./minio-buckets-job.yaml`](./minio-buckets-job.yaml)
- Incident: `cluster-config/runlogs/MIGRATION-3node-2026-05-23/CHRONOLOGY.md` —
  the **2026-06-17 MinIO expand + Spark-History-Server fix** entry.
