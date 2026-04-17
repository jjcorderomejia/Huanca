# Real-Time Payment Fraud Detection Pipeline
## Complete Lab — kubeadm Kubernetes | Production-Style | 2026 Stack
### Repo: Huanca | Cluster: Hetzner | Registry: ghcr.io/jjcorderomejia

---

## V9 

--

## STACK OVERVIEW

| Layer | Technology | Replaces |
|---|---|---|
| Streaming | Redpanda | Kafka + Strimzi |
| Compute | Spark Structured Streaming (Spark Operator) | unchanged |
| Real-time sink | StarRocks | Cassandra |
| Object storage | MinIO | HDFS |
| Table format | Apache Iceberg (JDBC catalog on PostgreSQL) | Hive Metastore |
| Orchestration | Airflow (K8sExecutor) | none |
| Build engine | BuildKit | Kaniko (archived) |
| GitOps | ArgoCD | none |
| IaC baseline | Terraform (runs as K8s Job) | none |
| UI | React + Nginx | Tomcat JSP |
| Backend API | FastAPI | unchanged |

---

## GROUND RULES (READ BEFORE ANYTHING)

```
ENGINEERING DISCIPLINE:
1.  Every shell script starts with: set -euo pipefail
2.  Every image is tagged with GIT_SHA — :latest is NEVER used
3.  Every PVC explicitly sets storageClassName: local-path
4.  SparkApplication is NEVER managed by ArgoCD — always imperative
5.  BuildKit Jobs are one-shot per GIT_SHA — never reused
6.  Terraform state lives in MinIO — never on local disk
7.  Spark checkpoints go to MinIO — never to a ReadWriteOnce PVC
8.  All manifests use .yaml.tpl + envsubst → _rendered/ pattern
9.  UUID4 for all IDs — $RANDOM is banned
10. Git authentication via SSH key only — no plaintext tokens ever

SECURITY (NON-NEGOTIABLE):
11. Zero plaintext credentials in any YAML, Python, or Terraform file
12. All credentials live in Kubernetes Secrets — auto-generated via openssl rand (preferred) or read -s for external tokens (e.g. GHCR PAT)
13. All credentials mounted as env vars from K8s Secrets — never hardcoded
14. No default passwords — every service gets a strong password on first boot
15. No empty passwords — StarRocks, MinIO, all services require real auth
16. sensitive=true on all Terraform credential variables
17. .gitignore covers: _rendered/, *.tfstate, *.tfvars, .env, *secret* files

VALIDATION (NON-NEGOTIABLE):
18. kubectl port-forward is NEVER used as a validation step in this lab
19. Internal services validated via ClusterIP curl from the kubeadm host
20. User-facing services validated via Ingress + public IP
```

---

## NAMESPACE MAP

```
bigdata/        infra + compute (Redpanda, StarRocks, MinIO, Spark, Airflow)
apps/           user-facing (FastAPI, React UI, Ingress)
argocd/         GitOps controller
spark-operator/ Spark Operator controller
ingress-nginx/  Ingress controller (existing)
```

---

## REPO STRUCTURE (Huanca)



```
Huanca/
  infra/terraform/
    main.tf
    variables.tf
    outputs.tf
    backend.tf

  gitops/
    argocd/
      app-bigdata.yaml
    bigdata/
      redpanda/
        cluster.yaml
        topics-fraud.yaml
      starrocks/
        starrocks.yaml
        init-ddl-job.yaml
      minio/
        minio.yaml
      airflow/
        airflow.yaml
      enrichment/
        customer-csv-configmap.yaml

  k8s-apps/
    backend-api/
      Dockerfile
      buildkit-job.yaml.tpl
      backend-api.yaml.tpl
    fraud-ui/
      Dockerfile
      buildkit-job.yaml.tpl
      fraud-ui.yaml.tpl
    apps-ingress-ui.yaml
    apps-ingress-backend.yaml

  k8s-bigdata/
    spark-fraud-job/
      Dockerfile
      buildkit-job.yaml.tpl
      spark-fraud-stream.yaml.tpl
      _rendered/
    _render_spark_ver/
      spark_base_image.env

  spark-jobs/
    fraud_stream_to_starrocks.py
    customer_csv_to_iceberg.py
    compact_iceberg.py

  docs/
    queries.md
    semantics.md
    system_design.md
    interview_talk_track.md
```

---

## 00-A) PER-SHELL ENV (run every login)

```bash

cat > ~/.lab_Huanca  <<'EOF'
set -euo pipefail

export REGISTRY=ghcr.io
export ORG=jjcorderomejia
export IMAGE_NS=$REGISTRY/$ORG
export API_SERVER=https://95.217.112.184:6443
export SPARK_MASTER=k8s://https://95.217.112.184:6443

export REPO_ROOT=/home/jjcm/Huanca
export BD_ROOT=$REPO_ROOT/k8s-bigdata
export APPS_ROOT=$REPO_ROOT/k8s-apps
export HOST_BACKEND_ROOT=$REPO_ROOT/k8s-apps/backend-api
export HOST_SPARK_JOB_ROOT=$REPO_ROOT/k8s-bigdata/spark-fraud-job
export HOST_UI_ROOT=$REPO_ROOT/k8s-apps/fraud-ui

# GIT_SHA from repo
export GIT_SHA=$(git -C $REPO_ROOT rev-parse --short HEAD)
echo "GIT_SHA=${GIT_SHA}"

# Namespaces
export NS_BIG=bigdata
export NS_APP=apps
EOF

```


---

## 00-B: GITHUB SSH SETUP + REPO INITIALIZATION
## (Run once — before anything else in this lab)

> Prod standard: zero plaintext tokens. SSH key authentication only.
> Never run: git remote set-url origin https://YOUR_TOKEN@github.com/...

### Step 1 — Generate SSH key on Hetzner

```bash
ssh-keygen -t ed25519 -C "hetzner-huanca" -f ~/.ssh/github_huanca -N ""
cat ~/.ssh/github_huanca.pub
```

Copy the full output — you paste it into GitHub in the next step.

### Step 2 — Add public key to GitHub

```
1. Open: https://github.com/settings/ssh/new
2. Title: hetzner-huanca
3. Key:   paste the output from Step 1
4. Click: Add SSH key
```

### Step 3 — Configure SSH on Hetzner

```bash
cat >> ~/.ssh/config <<'EOF'
Host github.com
  IdentityFile ~/.ssh/github_huanca
  User git
EOF
chmod 600 ~/.ssh/config
```

### Step 4 — Test SSH authentication

```bash
ssh -T git@github.com
# Expected: Hi jjcorderomejia! You've successfully authenticated
```

Do not proceed until you see that message.

### Step 5 — Initialize Huanca repo + first commit

```bash
set -euo pipefail

# Git identity (one-time global config)
git config --global user.email "jjcorderomejia@yahoo.com"
git config --global user.name "jjcorderomejia"

# Create full repo folder structure
mkdir -p ~/Huanca/{infra/terraform,gitops/{argocd,bigdata/{redpanda,starrocks,minio,airflow,enrichment}},k8s-apps/{backend-api,fraud-ui},k8s-bigdata/spark-fraud-job/_rendered,spark-jobs,docs}

# .gitignore — never commit rendered manifests, state files, or secrets
cat > ~/Huanca/.gitignore <<'EOF'
# Rendered manifests (contain image SHAs — never commit)
**/_rendered/

# Terraform state (lives in MinIO — never local)
**/*.tfstate
**/*.tfstate.backup
**/.terraform/
**/*.tfvars

# Secrets and env files
.env
*.env
*secret*
*credentials*

# Python
__pycache__/
*.pyc
*.pyo
EOF

# README placeholder
cat > ~/Huanca/README.md <<'EOF'
# Huanca — Real-Time Payment Fraud Detection Pipeline

Kubernetes-native fraud detection lab.

Stack: Redpanda | Spark Structured Streaming | StarRocks | Apache Iceberg | MinIO | Airflow | ArgoCD | BuildKit

> Lab in progress
EOF

cd ~/Huanca
git init
git branch -M main
git remote add origin git@github.com:jjcorderomejia/Huanca.git

git add .
git commit -m "Initial commit — Huanca fraud detection lab structure"
git push -u origin main

echo "✅ Repo live: https://github.com/jjcorderomejia/Huanca"
```

---

## 00-C: CLEAN RESET (run if cluster state is dirty)

```bash
#!/bin/bash
set -euo pipefail

for NS in bigdata apps argocd; do
  echo "🧹 Resetting namespace: ${NS}"
  kubectl -n "${NS}" delete deploy,sts,ds,rs,po,svc,ing,job,cronjob \
    --all --ignore-not-found
  kubectl -n "${NS}" delete cm,secret,sa,role,rolebinding \
    --all --ignore-not-found
  kubectl -n "${NS}" delete pvc --all --ignore-not-found
  kubectl delete ns "${NS}" --ignore-not-found
  echo "✅ Done: ${NS}"
done
```


---

## 00-D: HOST TOOLS
## (Run once — host-level tools required by later phases)

```bash
# Node 20 LTS — required by Phase 8 to generate package-lock.json
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get remove -y libnode-dev libnode72 nodejs --ignore-missing
sudo apt-get install -y nodejs
node --version
npm --version
```

---

## ════════════════════════════════════════
## PHASE 0 — TERRAFORM BASELINE
## ════════════════════════════════════════

> Provisions: namespaces, ServiceAccounts, RBAC, GHCR pull secrets.
> State lives in MinIO (bootstrapped below).
> Terraform runs as a K8s Job — cluster DNS resolves naturally.
> Safe to re-run (terraform apply is idempotent).

### Secrets Reference

| Secret | Auto-generated? | Method | Why |
|--------|----------------|--------|-----|
| `starrocks-credentials` | ✅ | `openssl rand` | We set the StarRocks root password |
| `airflow-admin-credentials` | ✅ | `openssl rand` | We set the Airflow admin password |
| `airflow-webserver-secret` | ✅ | `openssl rand` | We set the Airflow webserver key |
| `api-key-secret` | ✅ | `openssl rand` | We issue our own API key |
| `minio-secret` | ✅ | Pre-existing | Inherited from Cassandra lab |
| `ghcr-creds` | ❌ | `terraform-vars` | Issued by GitHub — external PAT |
| `terraform-vars` (`ghcr-token`) | ❌ | `read -s` once at bootstrap | Issued by GitHub — external PAT |
| `terraform-vars` (`hcloud-token`) | ❌ | `read -s` once at bootstrap | Issued by Hetzner Cloud — external API token |

### 0.1 Install Terraform on host (optional — for debugging only)

```bash
# Terraform runs as a K8s Job, but having it on the host is useful for
# terraform output, terraform state list, etc.
sudo apt-get update -y
sudo apt-get install -y gnupg software-properties-common curl

curl -fsSL https://apt.releases.hashicorp.com/gpg \
  | gpg --dearmor \
  | sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg >/dev/null

echo "deb [arch=$(dpkg --print-architecture) \
  signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
  https://apt.releases.hashicorp.com \
  $(. /etc/os-release && echo ${UBUNTU_CODENAME:-$VERSION_CODENAME}) main" \
  | sudo tee /etc/apt/sources.list.d/hashicorp.list >/dev/null

sudo apt-get update -y
sudo apt-get install -y terraform

terraform -version
```

### 0.2 MinIO — Already Running (SKIP DEPLOYMENT)

> MinIO is already deployed in the `bigdata` namespace from the Cassandra lab.
> StatefulSet: `minio` | Secret: `minio-secret` | Keys: `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`
> All fraud lab manifests reference `minio-secret` directly — no alias secret needed.

```bash
# Verify MinIO is healthy before continuing
kubectl -n bigdata get pods -l app=minio
kubectl -n bigdata get statefulset minio
echo "✅ MinIO running — proceed to 0.3"
```

### 0.3 Create MinIO buckets (idempotent via mc)

> Creates fraud lab buckets on the existing MinIO instance.
> Credentials sourced directly from `minio-secret` (keys: MINIO_ROOT_USER / MINIO_ROOT_PASSWORD).

```bash
kubectl -n bigdata run mc-setup --rm -it --restart=Never \
  --image=quay.io/minio/mc:RELEASE.2024-06-12T14-34-03Z \
  --env="MINIO_USER=$(kubectl -n bigdata get secret minio-secret \
    -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)" \
  --env="MINIO_PASS=$(kubectl -n bigdata get secret minio-secret \
    -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)" \
  --command -- bash -lc '
    set -euo pipefail
    mc alias set local http://minio:9000 "${MINIO_USER}" "${MINIO_PASS}"

    # Create buckets (idempotent — safe to re-run)
    for bucket in iceberg tf-state checkpoints airflow; do
      mc mb --ignore-existing local/$bucket
      echo "✅ bucket: $bucket"
    done

    mc ls local
  '

# Commit
cd $REPO_ROOT
export GIT_SHA=$(git -C $REPO_ROOT rev-parse --short HEAD)
git add .
git commit -m "phase-0.3: MinIO fraud lab buckets created"
git push origin main  
```

### 0.4 Write Terraform files

| File | Purpose |
|---|---|
| `backend.tf` | Where Terraform stores state (MinIO `tf-state` bucket) |
| `variables.tf` | Input parameters — no hardcoded values in resources |
| `main.tf` | The actual infrastructure — what Terraform creates. In this lab: Kubernetes namespaces, RBAC (ServiceAccounts, Roles, RoleBindings), PVCs.  |
| `outputs.tf` | What Terraform prints after apply — values used in later phases |

```bash
mkdir -p $REPO_ROOT/infra/terraform
```

#### `infra/terraform/backend.tf`

```bash
# FIX 1+2: Terraform runs as a K8s Job — endpoint uses cluster DNS.
# FIX 2: Uses current endpoints {} block + use_path_style (not deprecated endpoint/force_path_style).
cat > $REPO_ROOT/infra/terraform/backend.tf <<'EOF'
terraform {
  backend "s3" {
    bucket                      = "tf-state"
    key                         = "fraud-lab/terraform.tfstate"
    region                      = "us-east-1"
    endpoints                   = { s3 = "http://minio.bigdata.svc.cluster.local:9000" }
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_requesting_account_id  = true
    use_path_style              = true
    # Credentials passed via AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars
    # Never hardcoded here — injected from minio-secret in K8s Job
  }
}
EOF
```

#### `infra/terraform/variables.tf`

```bash
cat > $REPO_ROOT/infra/terraform/variables.tf <<'EOF'
variable "ghcr_token" {
  type      = string
  sensitive = true
}

variable "ghcr_user" {
  type    = string
  default = "jjcorderomejia"
}

variable "bigdata_namespace" {
  type    = string
  default = "bigdata"
}

variable "apps_namespace" {
  type    = string
  default = "apps"
}

# Required when any hcloud_* resource or data source is managed.
# Inject as TF_VAR_hcloud_token — never hardcoded.
variable "hcloud_token" {
  description = "Hetzner Cloud API token."
  type        = string
  sensitive   = true
}
EOF
```

#### `infra/terraform/main.tf`

```bash
cat > $REPO_ROOT/infra/terraform/main.tf <<'EOF'
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.20.0"
    }
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = ">= 1.45.0"
    }
  }
}

provider "kubernetes" {}

# ── Hetzner Cloud provider ─────────────────────────────────────────────
# Manages node lifecycle via cloud-init user_data on new Hetzner servers.
# Token injected as TF_VAR_hcloud_token — never hardcoded.
provider "hcloud" {
  token = var.hcloud_token
}

# ── Namespaces (all pre-existing — Terraform reads only) ─────────────
data "kubernetes_namespace" "bigdata" {
  metadata { name = var.bigdata_namespace }
}

data "kubernetes_namespace" "apps" {
  metadata { name = var.apps_namespace }
}

data "kubernetes_namespace" "argocd" {
  metadata { name = "argocd" }
}

data "kubernetes_namespace" "spark_operator" {
  metadata { name = "spark-operator" }
}

# ── Existing secrets — read only ────────────────────────────────────
data "kubernetes_secret" "ghcr_bigdata" {
  metadata {
    name      = "ghcr-creds"
    namespace = "bigdata"
  }
}

data "kubernetes_secret" "ghcr_apps" {
  metadata {
    name      = "ghcr-creds"
    namespace = "apps"
  }
}

# ── Spark ServiceAccount + RBAC ─────────────────────────────────────
data "kubernetes_service_account" "spark" {
  metadata {
    name      = "spark"
    namespace = "bigdata"
  }
}

resource "kubernetes_cluster_role" "spark_role" {
  metadata { name = "spark-role" }
  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "services", "configmaps", "secrets",
                  "persistentvolumeclaims"]
    verbs      = ["get", "list", "watch", "create", "delete", "deletecollection", "patch", "update"]
  }
  rule {
    api_groups = ["sparkoperator.k8s.io"]
    resources  = ["sparkapplications", "scheduledsparkapplications"]
    verbs      = ["get", "list", "watch", "create", "delete", "patch", "update"]
  }
}

resource "kubernetes_cluster_role_binding" "spark_binding" {
  metadata { name = "spark-role-binding" }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.spark_role.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = data.kubernetes_service_account.spark.metadata[0].name
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}

# ── BuildKit ServiceAccount + RBAC ──────────────────────────────────
resource "kubernetes_service_account" "buildkit" {
  metadata {
    name      = "buildkit"
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}

resource "kubernetes_cluster_role" "buildkit_role" {
  metadata { name = "buildkit-role" }
  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log"]
    verbs      = ["get", "list", "watch", "create", "delete"]
  }
}

resource "kubernetes_cluster_role_binding" "buildkit_binding" {
  metadata { name = "buildkit-role-binding" }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.buildkit_role.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.buildkit.metadata[0].name
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}

# ── Airflow ServiceAccount + RBAC ───────────────────────────────────
resource "kubernetes_service_account" "airflow" {
  metadata {
    name      = "airflow"
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}

resource "kubernetes_cluster_role" "airflow_role" {
  metadata { name = "airflow-role" }
  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "pods/exec"]
    verbs      = ["get", "list", "watch", "create", "delete", "patch"]
  }
  rule {
    api_groups = ["sparkoperator.k8s.io"]
    resources  = ["sparkapplications", "sparkapplications/status", "scheduledsparkapplications"]
    verbs      = ["get", "list", "watch", "create", "delete", "patch", "update"]
  }
}

resource "kubernetes_cluster_role_binding" "airflow_binding" {
  metadata { name = "airflow-role-binding" }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.airflow_role.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.airflow.metadata[0].name
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}

resource "kubernetes_cluster_role_binding" "airflow_worker_binding" {
  metadata { name = "airflow-worker-role-binding" }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.airflow_role.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = "airflow-worker"
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}
EOF
```

#### `infra/terraform/outputs.tf`

```bash
cat > $REPO_ROOT/infra/terraform/outputs.tf <<'EOF'
output "namespaces" {
  value = [
    data.kubernetes_namespace.bigdata.metadata[0].name,
    data.kubernetes_namespace.apps.metadata[0].name,
    data.kubernetes_namespace.argocd.metadata[0].name,
    data.kubernetes_namespace.spark_operator.metadata[0].name,
  ]
  description = "All namespaces — pre-existing, Terraform reads only"
}
EOF
```

### 0.5 Apply Terraform (as K8s Job)

> FIX 1: Terraform runs inside the cluster as a K8s Job.
> Cluster DNS resolves minio.bigdata.svc.cluster.local naturally.
> Kubernetes provider uses in-cluster ServiceAccount auth — no kubeconfig.
> This is the Atlantis/Spacelift pattern used at FAANG scale.

```bash
set -euo pipefail

# Create Terraform ServiceAccount with cluster-admin
# (needs to create namespaces, ClusterRoles, ClusterRoleBindings)
kubectl apply -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: terraform
  namespace: bigdata
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: terraform-admin
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
  - kind: ServiceAccount
    name: terraform
    namespace: bigdata
EOF

# Store credentials in K8s Secret — never in shell history or Job YAML
# Both tokens are typed once at bootstrap and never re-entered.
# Fresh install: type both tokens.
# Re-run (terraform-vars already exists): only type hcloud-token;
#   GHCR_TOKEN is read from the existing secret automatically.

# Fresh install only — skip if terraform-vars already has ghcr-token:
read -s -p "Enter GHCR token (fresh install only — press Enter to skip): " GHCR_TOKEN
echo

# Always required — Hetzner Cloud API token:
read -s -p "Enter Hetzner Cloud API token: " HCLOUD_TOKEN
echo

# If skipped above, read ghcr-token from existing secret
if [ -z "${GHCR_TOKEN:-}" ]; then
  GHCR_TOKEN=$(kubectl -n bigdata get secret terraform-vars -o jsonpath='{.data.ghcr-token}' | base64 -d)
fi

kubectl -n bigdata create secret generic terraform-vars \
  --from-literal=ghcr-token="${GHCR_TOKEN}" \
  --from-literal=hcloud-token="${HCLOUD_TOKEN}" \
  --dry-run=client -o yaml | kubectl apply -f -

unset GHCR_TOKEN HCLOUD_TOKEN

# Run Terraform as a K8s Job
kubectl -n bigdata delete job terraform-apply --ignore-not-found

cat <<JOBEOF | kubectl apply -f -
apiVersion: batch/v1
kind: Job
metadata:
  name: terraform-apply
  namespace: bigdata
spec:
  backoffLimit: 2
  template:
    spec:
      serviceAccountName: terraform
      restartPolicy: Never
      containers:
        - name: terraform
          image: hashicorp/terraform:1.9.8
          workingDir: /terraform
          command: ["sh", "-c"]
          args:
            - |
              set -euo pipefail
              terraform init -reconfigure
              terraform apply -auto-approve
              echo "✅ Terraform apply complete"
              terraform output
          env:
            - name: AWS_ACCESS_KEY_ID
              valueFrom:
                secretKeyRef:
                  name: minio-secret
                  key: MINIO_ROOT_USER
            - name: AWS_SECRET_ACCESS_KEY
              valueFrom:
                secretKeyRef:
                  name: minio-secret
                  key: MINIO_ROOT_PASSWORD
            - name: TF_VAR_ghcr_token
              valueFrom:
                secretKeyRef:
                  name: terraform-vars
                  key: ghcr-token
            - name: TF_VAR_hcloud_token
              valueFrom:
                secretKeyRef:
                  name: terraform-vars
                  key: hcloud-token
          volumeMounts:
            - name: tf-files
              mountPath: /terraform
      volumes:
        - name: tf-files
          hostPath:
            path: ${REPO_ROOT}/infra/terraform
            type: Directory
JOBEOF

kubectl -n bigdata wait --for=condition=complete job/terraform-apply --timeout=300s
kubectl -n bigdata logs job/terraform-apply

#troubleshouting in case a terraform pod contains error, Fix tf, then clean the stale state and re-run:
kubectl -n bigdata delete job terraform-apply --ignore-not-found
# Re-run the Job creation block from V9 Phase 0.5

cd $REPO_ROOT
```

### 0.6 Node Configuration (Hetzner)

Containerd root defaults to `/var/lib/containerd` on the OS disk (~78 GB total). The `/data` block
device provides 462 GB. Moving containerd root to `/data` eliminates recurring DiskPressure events
as image layers accumulate across builds.

`infra/cloud-init/node.yaml` is the authoritative node bootstrap template — applied automatically
as `user_data` when provisioning new Hetzner nodes via the `hcloud` Terraform provider. For the
existing node, execute the one-time migration below.

#### Write infra/cloud-init/node.yaml

```bash
mkdir -p $REPO_ROOT/infra/cloud-init
cat > $REPO_ROOT/infra/cloud-init/node.yaml <<'EOF'
#cloud-config
# Node bootstrap for Hetzner servers in this lab.
# Sets containerd root to the large data disk (/data, 462 GB)
# to prevent DiskPressure on the OS disk as image layers accumulate.
write_files:
  - path: /etc/containerd/config.toml
    owner: root:root
    permissions: "0644"
    content: |
      version = 2
      root = "/data/containerd"
runcmd:
  - mkdir -p /data/containerd
  - systemctl restart containerd
EOF
```

#### Apply to existing node — one-time migration

> **Note:** containerd stops for ~5 seconds. Running containers survive (managed by the kernel
> directly); kubelet briefly loses CRI connectivity and recovers automatically on restart.

```bash
# 1. Stop containerd
sudo systemctl stop containerd

# 2. Move image + container data to /data
sudo mv /var/lib/containerd /data/containerd

# 3. Update config — root points to /data/containerd
sudo tee /etc/containerd/config.toml <<'EOF'
version = 2
root = "/data/containerd"
EOF

# 4. Restart containerd
sudo systemctl start containerd
```

```bash
# Verify
sudo systemctl is-active containerd
df -h /data
```

#### Migrate local-path-provisioner to /data — one-time migration

`local-path-provisioner` defaults to `/opt/local-path-provisioner` on the OS disk.
Same pattern as containerd: stop, move, reconfigure, restart.
Frees the OS disk from PVC data growth (MinIO alone can consume 79+ GB).

```bash
# 1. Scale down provisioner
kubectl -n local-path-storage scale deployment local-path-provisioner --replicas=0
kubectl -n local-path-storage wait --for=delete pod -l app=local-path-provisioner --timeout=60s 2>/dev/null || true
```

```bash
# 2. Move PVC data to /data (rsync required — /opt and /data are separate block devices)
sudo mkdir -p /data/local-path-provisioner
sudo rsync -a --remove-source-files /opt/local-path-provisioner/ /data/local-path-provisioner/
sudo find /opt/local-path-provisioner -type d -empty -delete
```

```bash
# 3. Update ConfigMap nodePathMap to /data/local-path-provisioner
kubectl -n local-path-storage get configmap local-path-config -o yaml \
  | sed 's|/opt/local-path-provisioner|/data/local-path-provisioner|g' \
  | kubectl apply -f -
```

```bash
# 4. Bind mount /data/local-path-provisioner onto /opt/local-path-provisioner
# PV spec is immutable — pods keep their /opt paths; bind mount makes /opt read from /data
sudo mkdir -p /opt/local-path-provisioner
sudo mount --bind /data/local-path-provisioner /opt/local-path-provisioner

# Persist across reboots
echo '/data/local-path-provisioner /opt/local-path-provisioner none bind 0 0' \
  | sudo tee -a /etc/fstab
echo "Bind mount active and persisted in /etc/fstab"
```

```bash
# 5. Restart provisioner
kubectl -n local-path-storage scale deployment local-path-provisioner --replicas=1
kubectl -n local-path-storage rollout status deployment/local-path-provisioner --timeout=60s
```

```bash
# Verify — OS disk should have 80+ GB freed
df -h /
kubectl get pv | head -5
```

```bash
# Recovery only — NOT required on clean setups.
# If StatefulSets started before bind mount was active (e.g. FE restarted during migration),
# they will have initialised against an empty /opt path. Restart them to pick up restored data.
kubectl -n bigdata rollout restart statefulset/starrocks-fe
kubectl -n bigdata rollout status statefulset/starrocks-fe --timeout=120s
```

> **Recovery only — NOT required on clean setups.**
> If FE is stuck at BDB setup ("New node unknown to rep group"), the BDB node name recorded
> during the empty-data restart conflicts with the restored journals. Fix: wipe BDB journals
> so FE rebuilds metadata from the image snapshot. Data is NOT lost — the image file contains
> a complete snapshot of all metadata.

```bash
FE_PV=$(kubectl -n bigdata get pvc fe-data-starrocks-fe-0 -o jsonpath='{.spec.volumeName}')
FE_PVC_PATH="/opt/local-path-provisioner/${FE_PV}_bigdata_fe-data-starrocks-fe-0"
kubectl -n bigdata scale statefulset/starrocks-fe --replicas=0
kubectl -n bigdata wait --for=delete pod/starrocks-fe-0 --timeout=60s 2>/dev/null || true
sudo find "${FE_PVC_PATH}/bdb" -name "*.jdb" -delete
sudo rm -f "${FE_PVC_PATH}/bdb/je.lck" "${FE_PVC_PATH}/bdb/je.info.0.lck"
kubectl -n bigdata patch statefulset starrocks-fe --type=json -p='[{
  "op": "add",
  "path": "/spec/template/spec/containers/0/command",
  "value": ["/bin/bash", "-c"]
},{
  "op": "add",
  "path": "/spec/template/spec/containers/0/args",
  "value": ["echo '\''start_with_incomplete_meta = true'\'' >> /opt/starrocks/fe/conf/fe.conf && exec /opt/starrocks/fe_entrypoint.sh starrocks-fe-svc"]
}]'
kubectl -n bigdata scale statefulset/starrocks-fe --replicas=1
kubectl -n bigdata rollout status statefulset/starrocks-fe --timeout=120s
```

> **Recovery only — remove StatefulSet command patch once FE is healthy.**
> `start_with_incomplete_meta` must not persist — it forces image-only recovery on every restart.

```bash
kubectl -n bigdata patch statefulset starrocks-fe --type=json -p='[
  {"op": "replace", "path": "/spec/template/spec/containers/0/command", "value": ["/bin/bash", "-c"]},
  {"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": ["exec /opt/starrocks/fe_entrypoint.sh starrocks-fe-svc"]}
]'
kubectl -n bigdata rollout status statefulset/starrocks-fe --timeout=120s
```

> **Recovery only — reset BE cluster ID after FE BDB rebuild.**
> FE generates a new cluster ID after `start_with_incomplete_meta` recovery. The BE's stored
> `cluster_id` file is stale and causes `Unmatched cluster id`. Fix: scale down BE, wipe cluster_id,
> drop all stale BE records from FE, scale up — BE re-registers with the new cluster ID on startup.

```bash
BE_PV=$(kubectl -n bigdata get pvc be-data-starrocks-be-0 -o jsonpath='{.spec.volumeName}')
BE_PVC_PATH="/opt/local-path-provisioner/${BE_PV}_bigdata_be-data-starrocks-be-0"
SR_PASS=$(kubectl -n bigdata get secret starrocks-credentials -o jsonpath='{.data.root-password}' | base64 -d)
kubectl -n bigdata scale statefulset/starrocks-be --replicas=0
kubectl -n bigdata wait --for=delete pod/starrocks-be-0 --timeout=60s 2>/dev/null || true
sudo rm -f "${BE_PVC_PATH}/cluster_id"
DEAD_BES=$(kubectl -n bigdata exec -i starrocks-fe-0 -- env MYSQL_PWD="${SR_PASS}" mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G" 2>&1 | awk '/IP:/{ip=$2} /HeartbeatPort:/{port=$2} /Alive: false/{print ip":"port}')
for be in $DEAD_BES; do
  kubectl -n bigdata exec -i starrocks-fe-0 -- env MYSQL_PWD="${SR_PASS}" mysql -h 127.0.0.1 -P 9030 -u root -e "ALTER SYSTEM DROP BACKEND \"${be}\" FORCE" 2>/dev/null || true
done
kubectl -n bigdata scale statefulset/starrocks-be --replicas=1
kubectl -n bigdata wait --for=condition=Ready pod/starrocks-be-0 --timeout=120s
```

```bash
SR_PASS=$(kubectl -n bigdata get secret starrocks-credentials -o jsonpath='{.data.root-password}' | base64 -d)
kubectl -n bigdata exec -i starrocks-fe-0 -- env MYSQL_PWD="${SR_PASS}" mysql -h 127.0.0.1 -P 9030 -u root -e "SHOW BACKENDS\G" 2>&1 | grep -E "BackendId|IP|Alive|LastHeartbeat"
```

> **Recovery only — restart MinIO after local-path-provisioner migration.**
> MinIO reports `listPathRaw: 0 drives provided` after the bind mount migration because the
> pod's volume path is stale. A pod restart forces MinIO to re-scan its drives from the
> current mount and recover.

```bash
kubectl -n bigdata delete pod minio-0
kubectl -n bigdata wait --for=condition=Ready pod/minio-0 --timeout=120s
```

> **Recovery only — rebuild StarRocks tablets after full BE drop.**
> Dropping all backends resets tablet assignments. The StarRocks tables
> `transactions`, `risk_profiles`, and `fraud_scores` become invalid
> (`OlapScanNode fail`). Fix: truncate all three and re-run the streaming
> jobs to repopulate tablets from scratch. Note: `customers` is an Iceberg
> table in MinIO — not a StarRocks table; it is unaffected.

```bash
SR_PASS=$(kubectl -n bigdata get secret starrocks-credentials -o jsonpath='{.data.root-password}' | base64 -d)
kubectl -n bigdata exec -i starrocks-fe-0 -- env MYSQL_PWD="${SR_PASS}" mysql -h 127.0.0.1 -P 9030 -u root <<SQL
USE fraud;
TRUNCATE TABLE transactions;
TRUNCATE TABLE risk_profiles;
TRUNCATE TABLE fraud_scores;
SQL
```

> **Recovery only — wipe Spark checkpoint after Redpanda storage reset.**
> After a Redpanda storage crash, topic offsets reset to 0 while the Spark
> checkpoint in MinIO still holds old (higher) offsets. Spark stalls waiting
> for offsets that no longer exist (`partitions are gone` WARN, `total: 0`
> in all batches). Fix: delete the checkpoint bucket path in MinIO and
> restart spark-fraud-stream so it resumes from `earliest` (offset 0).
> Checkpoint rebuilds automatically on the first micro-batch after restart.

```bash
MINIO_USER=$(kubectl -n bigdata get secret minio-secret -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)
MINIO_PASS=$(kubectl -n bigdata get secret minio-secret -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)
kubectl -n bigdata exec -i minio-0 -- env MC_HOST_local="http://${MINIO_USER}:${MINIO_PASS}@localhost:9000" \
  mc rm --recursive --force local/checkpoints/fraud-stream-v2
echo "✅ Spark checkpoint wiped — stream will resume from earliest on next start"
```

#### Commit 0.6

```bash
cd $REPO_ROOT
git add infra/cloud-init/node.yaml
git add infra/terraform/variables.tf
git add infra/terraform/main.tf
git commit -m "Phase 0.6: hcloud provider + node cloud-init (containerd root → /data)"
```

### 0.7 Validate Phase 0

```bash
# Namespaces
kubectl get ns | grep -E "bigdata|apps|argocd|spark-operator"

# Service accounts
kubectl -n bigdata get sa | grep -E "spark|buildkit|airflow"

# GHCR secrets
kubectl -n bigdata get secret ghcr-creds -o jsonpath='{.type}{"\n"}'
kubectl -n apps   get secret ghcr-creds -o jsonpath='{.type}{"\n"}'

# MinIO running
kubectl -n bigdata get pods -l app=minio
```

### 0.8 Commit Phase 0 to GitHub

```bash
# Commit
cd $REPO_ROOT
export GIT_SHA=$(git -C $REPO_ROOT rev-parse --short HEAD)
git add .
git commit -m "Phase 0: Terraform baseline (K8s Job) + MinIO bootstrap"
git push origin main  
```

---

## ════════════════════════════════════════
## PHASE 1 — REDPANDA CLUSTER + TOPICS
## ════════════════════════════════════════

> Redpanda is Kafka-compatible. No ZooKeeper. No Strimzi.
> Your existing Spark Kafka connector works with zero changes.

### 1.1 Install Redpanda Operator (idempotent)

```bash
# Add Redpanda helm repo
helm repo add redpanda https://charts.redpanda.com
helm repo update

# Install operator
helm upgrade --install redpanda-operator redpanda/operator \
  --namespace bigdata \
  --version 25.2.1 \
  --set image.repository=docker.io/redpandadata/redpanda-operator \
  --set additionalCmdFlags="{--enable-console=false}" \
  --wait --timeout=300s
```

## ════════════════════════════════════════
## PHASE 1 — REDPANDA CLUSTER + TOPICS
## ════════════════════════════════════════

>  Redpanda is Kafka-compatible. No ZooKeeper. No Strimzi.
>  Existing Spark Kafka connector works with zero changes.

## 1.1 Install Redpanda Operator (idempotent)


```bash

set -euo pipefail

REDPANDA_OPERATOR_VERSION="25.2.1"
CRD_BASE="https://raw.githubusercontent.com/redpanda-data/redpanda-operator/operator/v${REDPANDA_OPERATOR_VERSION}/operator/config/crd/bases"

## Step 1 — Apply all CRDs.
## The redpanda/operator helm chart (v25.x) does NOT bundle CRDs. They must
## be applied from the matching source tag BEFORE the operator starts.
## A version mismatch between CRDs and operator binary causes reconcile failures.
##
## LAB NOTE: CRDs are fetched directly from GitHub at runtime. In a production
## environment, mirror these to an internal artifact store and verify checksums
## before applying.

echo "[1/3] Applying Redpanda CRDs (operator v${REDPANDA_OPERATOR_VERSION})..."

kubectl apply --server-side \
  -f "${CRD_BASE}/cluster.redpanda.com_consoles.yaml" \
  -f "${CRD_BASE}/cluster.redpanda.com_nodepools.yaml" \
  -f "${CRD_BASE}/cluster.redpanda.com_redpandas.yaml" \
  -f "${CRD_BASE}/cluster.redpanda.com_redpandaroles.yaml" \
  -f "${CRD_BASE}/cluster.redpanda.com_schemas.yaml" \
  -f "${CRD_BASE}/cluster.redpanda.com_shadowlinks.yaml" \
  -f "${CRD_BASE}/cluster.redpanda.com_topics.yaml" \
  -f "${CRD_BASE}/cluster.redpanda.com_users.yaml" \
  -f "${CRD_BASE}/redpanda.vectorized.io_clusters.yaml" \
  -f "${CRD_BASE}/redpanda.vectorized.io_consoles.yaml"

## Step 2 — Wait for CRDs to reach Established state.

echo "[2/3] Waiting for CRDs to be Established..."

kubectl wait --for=condition=Established --timeout=60s \
  crd/consoles.cluster.redpanda.com \
  crd/nodepools.cluster.redpanda.com \
  crd/redpandas.cluster.redpanda.com \
  crd/redpandaroles.cluster.redpanda.com \
  crd/schemas.cluster.redpanda.com \
  crd/shadowlinks.cluster.redpanda.com \
  crd/topics.cluster.redpanda.com \
  crd/users.cluster.redpanda.com \
  crd/clusters.redpanda.vectorized.io \
  crd/consoles.redpanda.vectorized.io

## Step 3 — Install (or upgrade) the operator.
##
## --force-update on repo add makes this idempotent regardless of prior state.
##
## `upgrade --install` is the idempotent form: installs on first run,
## upgrades on subsequent runs. Safe to re-run in CI/CD pipelines.
##
## NOTE: do NOT pass additionalCmdFlags="{--enable-console=false}".
## The helm template merges additionalCmdFlags with lower priority than its
## hardcoded defaults — the flag is silently ignored. The Console controller
## is harmless once the Console CRD exists (applied above).

echo "[3/3] Installing Redpanda operator..."

helm repo add redpanda https://charts.redpanda.com --force-update
helm repo update redpanda

helm upgrade --install redpanda-operator redpanda/operator \
  --namespace bigdata \
  --version "${REDPANDA_OPERATOR_VERSION}" \
  --set image.repository=docker.io/redpandadata/redpanda-operator \
  --wait --timeout=300s

## Verify:
kubectl -n bigdata get pods | grep redpanda-operator   # expect 1/1 Running
kubectl -n bigdata logs -l app.kubernetes.io/name=redpanda-operator \
| grep '"level":"error"' | grep -v WARNING            # expect no output


# Troubleshooting for deleting panda installation and runnign pods
kubectl -n bigdata describe pod redpanda-operator-847cdb5f55-rzd75 | grep -A5 "Events\|Failed\|pulling\|image"
kubectl -n bigdata logs redpanda-operator-9598f4f5-69nxl --previous
helm uninstall redpanda-operator -n bigdata
kubectl -n bigdata delete pod -l app.kubernetes.io/name=operator --force --grace-period=0  

kubectl -n bigdata rollout status deploy/redpanda-operator --timeout=180s

# Verify CRDs
kubectl get crd | grep redpanda
```

### 1.2 Deploy Redpanda Cluster

```bash
mkdir -p $REPO_ROOT/gitops/bigdata/redpanda

cat > $REPO_ROOT/gitops/bigdata/redpanda/cluster.yaml <<'EOF'
apiVersion: cluster.redpanda.com/v1alpha2
kind: Redpanda
metadata:
  name: fraud-redpanda
  namespace: bigdata
spec:
  chartRef: {}
  clusterSpec:
    statefulset:
      replicas: 1
    storage:
      persistentVolume:
        enabled: true
        size: 20Gi
        storageClass: local-path
    resources:
      requests:
        cpu: "250m"
        memory: "2Gi"
      limits:
        cpu: "2"
        memory: "2Gi"
    config:
      cluster:
        auto_create_topics_enabled: false
    listeners:
      kafka:
        port: 9092
EOF

kubectl apply -f $REPO_ROOT/gitops/bigdata/redpanda/cluster.yaml

# Wait for Redpanda to be ready
#kubectl -n bigdata wait \
#  --for=condition=ready pod \
#  -l app.kubernetes.io/name=redpanda \
#  --timeout=300s

kubectl -n bigdata wait redpanda/fraud-redpanda \
  --for=condition=Ready \
  --timeout=600s  

kubectl -n bigdata get pods | grep redpanda

#troubleshooting
kubectl -n bigdata logs fraud-redpanda-0 --previous | grep ERROR
kubectl -n bigdata logs fraud-redpanda-console-5998b4fc47-bhn7z
kubectl -n bigdata delete pod -l app.kubernetes.io/name=redpanda --force --grace-period=0
```

### 1.3 Create Fraud Topics

```bash
cat > $REPO_ROOT/gitops/bigdata/redpanda/topics-fraud.yaml <<'EOF'
apiVersion: cluster.redpanda.com/v1alpha2
kind: Topic
metadata:
  name: transactions-raw
  namespace: bigdata
spec:
  kafkaApiSpec:
    brokers:
      - fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092
    tls:
      enabled: true
      caCertSecretRef:
        name: fraud-redpanda-default-root-certificate
        key: ca.crt      
  partitions: 6
  replicationFactor: 1
  additionalConfig:
    retention.ms: "604800000"
    cleanup.policy: "delete"
---
apiVersion: cluster.redpanda.com/v1alpha2
kind: Topic
metadata:
  name: transactions-dlq
  namespace: bigdata
spec:
  kafkaApiSpec:
    brokers:
      - fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092
    tls:
      enabled: true
      caCertSecretRef:
        name: fraud-redpanda-default-root-certificate
        key: ca.crt       
  partitions: 3
  replicationFactor: 1
  additionalConfig:
    retention.ms: "1209600000"
    cleanup.policy: "delete"
---
apiVersion: cluster.redpanda.com/v1alpha2
kind: Topic
metadata:
  name: transactions-clean
  namespace: bigdata
spec:
  kafkaApiSpec:
    brokers:
      - fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092
    tls:
      enabled: true
      caCertSecretRef:
        name: fraud-redpanda-default-root-certificate
        key: ca.crt         
  partitions: 6
  replicationFactor: 1
  additionalConfig:
    retention.ms: "604800000"
    cleanup.policy: "delete"
---
apiVersion: cluster.redpanda.com/v1alpha2
kind: Topic
metadata:
  name: fraud-alerts
  namespace: bigdata
spec:
  kafkaApiSpec:
    brokers:
      - fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092
    tls:
      enabled: true
      caCertSecretRef:
        name: fraud-redpanda-default-root-certificate
        key: ca.crt         
  partitions: 3
  replicationFactor: 1
  additionalConfig:
    retention.ms: "604800000"
    cleanup.policy: "delete"
EOF

kubectl apply -f $REPO_ROOT/gitops/bigdata/redpanda/topics-fraud.yaml

kubectl wait topic/transactions-raw topic/transactions-dlq \
    topic/transactions-clean topic/fraud-alerts \
    -n bigdata --for=condition=Ready --timeout=60s

#Troubleshooting
kubectl -n bigdata get topics
kubectl -n bigdata describe topic transactions-raw
kubectl -n bigdata describe topic transactions-raw | grep -A5 Event    
```

### 1.4 Validate Phase 1

```bash
# Verify topics via rpk inside Redpanda pod
kubectl -n bigdata exec -it fraud-redpanda-0 -- rpk topic list

# Expect: transactions-raw, transactions-dlq, transactions-clean, fraud-alerts
```

### 1.5 Commit Phase 1 to GitHub

```bash
cd $REPO_ROOT
git add gitops/bigdata/redpanda/
git commit -m "Phase 1: Redpanda cluster + fraud topics"
git push origin main
```

---

## ════════════════════════════════════════
## PHASE 2 — STARROCKS DEPLOY + DDL
## ════════════════════════════════════════

> StarRocks replaces Cassandra.
> Primary Key tables = idempotent upserts.
> Real SQL. Sub-second analytics.

### 2.1 Deploy StarRocks (StatefulSet)

```bash
mkdir -p $REPO_ROOT/gitops/bigdata/starrocks

# FIX 9+10: Pinned image tags (was :3.2-latest)
cat > $REPO_ROOT/gitops/bigdata/starrocks/starrocks.yaml <<'EOF'
apiVersion: v1
kind: Service
metadata:
  name: starrocks-fe
  namespace: bigdata
spec:
  clusterIP: None
  selector:
    app: starrocks-fe
  ports:
    - name: http
      port: 8030
      targetPort: 8030
    - name: query
      port: 9030
      targetPort: 9030
    - name: rpc
      port: 9020
      targetPort: 9020
---
apiVersion: v1
kind: Service
metadata:
  name: starrocks-fe-svc
  namespace: bigdata
spec:
  selector:
    app: starrocks-fe
  ports:
    - name: http
      port: 8030
      targetPort: 8030
    - name: query
      port: 9030
      targetPort: 9030
    - name: rpc
      port: 9020
      targetPort: 9020      
---
apiVersion: v1
kind: Service
metadata:
  name: starrocks-be
  namespace: bigdata
spec:
  selector:
    app: starrocks-be
  clusterIP: None
  ports:
    - name: heartbeat
      port: 9050
      targetPort: 9050
    - name: be-port
      port: 9060
      targetPort: 9060
    - name: webserver
      port: 8040
      targetPort: 8040
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: starrocks-fe
  namespace: bigdata
spec:
  serviceName: starrocks-fe
  replicas: 1
  selector:
    matchLabels:
      app: starrocks-fe
  template:
    metadata:
      labels:
        app: starrocks-fe
    spec:
      containers:
        - name: fe
          image: starrocks/fe-ubuntu:3.2.11
          command: ["/opt/starrocks/fe_entrypoint.sh"]
          args: ["starrocks-fe-svc"]
          ports:
            - containerPort: 8030
            - containerPort: 9030
            - containerPort: 9020
          env:
            - name: HOST_TYPE
              value: "FQDN"
          volumeMounts:
            - name: fe-data
              mountPath: /opt/starrocks/fe/meta
          resources:
            requests:
              cpu: "500m"
              memory: "2Gi"
            limits:
              cpu: "2"
              memory: "4Gi"
  volumeClaimTemplates:
    - metadata:
        name: fe-data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: local-path
        resources:
          requests:
            storage: 10Gi
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: starrocks-be
  namespace: bigdata
spec:
  serviceName: starrocks-be
  replicas: 1
  selector:
    matchLabels:
      app: starrocks-be
  template:
    metadata:
      labels:
        app: starrocks-be
    spec:
      initContainers:
        - name: wait-fe
          image: busybox:1.36
          command: ['sh', '-c',
            'until nc -z starrocks-fe-svc 9030; do echo waiting for FE; sleep 3; done']
      containers:
        - name: be
          image: starrocks/be-ubuntu:3.2.11
          command: ["/opt/starrocks/be_entrypoint.sh"]
          args: ["starrocks-fe-svc"]
          ports:
            - containerPort: 9050
            - containerPort: 9060
            - containerPort: 8040
          env:
            - name: FE_SERVICE_NAME
              value: "starrocks-fe-svc"
            # Tech debt: be_entrypoint.sh uses `mysql -u root` with no password to register
            # with FE on every start. MYSQL_PWD is the standard MySQL client env var — the
            # only way to pass a password to a script that doesn't accept --password.
            # Required whenever BE restarts after root password is set (e.g. after eviction).
            - name: MYSQL_PWD
              valueFrom:
                secretKeyRef:
                  name: starrocks-credentials
                  key: root-password
          volumeMounts:
            - name: be-data
              mountPath: /opt/starrocks/be/storage
          resources:
            requests:
              cpu: "500m"
              memory: "2Gi"
            limits:
              cpu: "4"
              memory: "8Gi"
  volumeClaimTemplates:
    - metadata:
        name: be-data
      spec:
        accessModes: ["ReadWriteOnce"]
        storageClassName: local-path
        resources:
          requests:
            storage: 30Gi
EOF

kubectl apply -f $REPO_ROOT/gitops/bigdata/starrocks/starrocks.yaml

kubectl -n bigdata rollout status sts/starrocks-fe --timeout=300s
kubectl -n bigdata rollout status sts/starrocks-be --timeout=300s

#Troubleshooting

sudo crictl pull starrocks/fe-ubuntu:3.2.11
sudo crictl inspecti starrocks/fe-ubuntu:3.2.11 | python3 -m json.tool | grep -A5 -i "entrypoint\|cmd"

  # 1. All fixes on starrocks.yaml as detailed in Phase 2.1 Troubleshooting Summary

  # 2. Apply
  kubectl apply -f "$REPO_ROOT/gitops/bigdata/starrocks/starrocks.yaml"

  # 3. Delete BE pod to force recreation
  kubectl -n bigdata delete pod starrocks-be-0 --force --grace-period=0
```

  ### Phase 2.1 Troubleshooting Summary

  ### FE Issues

  | Issue | Root Cause | Fix |
  |---|---|---|
  | CrashLoopBackOff, exit code 0 | No entrypoint defined — CMD is `/bin/bash`, exits immediately with no TTY | Add `command:
  ["/opt/starrocks/fe_entrypoint.sh"]` to FE container |
  | `Need a required parameter` | Entrypoint requires FE service name as `$1` | Add `args: ["starrocks-fe-svc"]` |
  | `inotify instances reached` | 25+ pods on single node exhausted kernel default of 128 inotify instances | `sysctl
  fs.inotify.max_user_instances=8192` + persist in `/etc/sysctl.conf` |
  | BDB stuck in UNKNOWN / EnvironmentLocked | Stale `je.lck` lock files from crash runs left in PVC | Scale to 0, wipe entire
  `/opt/starrocks/fe/meta/` via busybox pod, scale back to 1 |
  | `current node is not added to cluster` | Partial BDB + image state caused node identity mismatch across restarts | Full meta wipe — BDB
  directory alone is not enough |

  ### BE Issues

  | Issue | Root Cause | Fix |
  |---|---|---|
  | `Init:0/1` stuck for 90+ minutes | Init container waiting on `starrocks-fe-svc:9030` while FE was crashing | Fix FE first — BE unblocks
  automatically |
  | CrashLoopBackOff after FE fixed | Same missing entrypoint args as FE | Add `command: ["/opt/starrocks/be_entrypoint.sh"]` + `args:
  ["starrocks-fe-svc"]` to BE container |

  ### Infrastructure Issues

  | Issue | Root Cause | Fix |
  |---|---|---|
  | FE StatefulSet had no headless service | Original manifest used ClusterIP service as `serviceName` — StatefulSets require headless | Add
  `clusterIP: None` to `starrocks-fe` + add separate `starrocks-fe-svc` ClusterIP service |

  ### Key Lesson
  `starrocks/fe-ubuntu` and `starrocks/be-ubuntu` images have **no default entrypoint**.
  Both require explicit `command` + `args` in the Kubernetes container spec.

---

### 2.2 Create StarRocks Secret + Automated Init Job

> Replaces all manual exec steps. Fully idempotent — safe to re-run.
> Job handles: wait for FE, set root password, register BE, verify.

```bash
# Create StarRocks credentials secret
# 1. Generate secret
kubectl -n bigdata create secret generic starrocks-credentials \
  --from-literal=root-password="$(openssl rand -base64 24)" \
  --dry-run=client -o yaml | kubectl apply -f -

# Deploy automated init Job
cat > $REPO_ROOT/gitops/bigdata/starrocks/starrocks-init-job.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: starrocks-init
  namespace: bigdata
spec:
  backoffLimit: 5
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: init
          image: starrocks/fe-ubuntu:3.2.11
          env:
            - name: SR_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: starrocks-credentials
                  key: root-password
          command: ["/bin/bash", "-c"]
          args:
            - |
              set -euo pipefail

              echo "⏳ Waiting for StarRocks FE..."
              until mysql -h starrocks-fe-svc -P 9030 -u root --password="" \
                --connect-timeout=5 -e "SELECT 1" >/dev/null 2>&1 || \
                mysql -h starrocks-fe-svc -P 9030 -u root \
                --password="${SR_PASSWORD}" \
                --connect-timeout=5 -e "SELECT 1" >/dev/null 2>&1; do
                echo "FE not ready, retrying in 5s..."
                sleep 5
              done
              echo "✅ FE is up"

              # Set root password — idempotent:
              # tries empty password first (fresh install), falls back to real password (re-run)
              mysql -h starrocks-fe-svc -P 9030 -u root --password="" \
                -e "ALTER USER root IDENTIFIED BY '${SR_PASSWORD}';" \
                2>/dev/null || \
              mysql -h starrocks-fe-svc -P 9030 -u root \
                --password="${SR_PASSWORD}" \
                -e "SELECT 'password already set';" >/dev/null
              echo "✅ Root password secured"

              # Register BE — idempotent (ADD BACKEND is safe to re-run)
              BE_IP=$(getent hosts starrocks-be-0.starrocks-be.bigdata.svc.cluster.local \
                | awk '{print $1}')

              mysql -h starrocks-fe-svc -P 9030 -u root \
                --password="${SR_PASSWORD}" \
                -e "ALTER SYSTEM ADD BACKEND '${BE_IP}:9050';" \
                2>/dev/null || echo "BE already registered"

              echo "✅ BE registered: ${BE_IP}"

              # Verify
              mysql -h starrocks-fe-svc -P 9030 -u root \
                --password="${SR_PASSWORD}" \
                -e "SHOW BACKENDS\G" | grep -E "Host|Alive"
EOF

kubectl apply -f $REPO_ROOT/gitops/bigdata/starrocks/starrocks-init-job.yaml

# Delete job, only for reexecutions
kubectl -n bigdata delete job starrocks-init

# 2. Watch job recover
kubectl -n bigdata get pods -l job-name=starrocks-init -w

kubectl -n bigdata wait \
  --for=condition=complete \
  job/starrocks-init \
  --timeout=300s

kubectl -n bigdata logs job/starrocks-init
```

### 2.3 DDL Job (idempotent — IF NOT EXISTS on all tables)

```bash
cat > $REPO_ROOT/gitops/bigdata/starrocks/init-ddl-job.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: starrocks-init-ddl
  namespace: bigdata
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: ddl
          image: starrocks/fe-ubuntu:3.2.11
          command: ["/bin/bash", "-c"]
          env:
            - name: SR_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: starrocks-credentials
                  key: root-password
          args:
            - |
              set -euo pipefail

              until mysql -h starrocks-fe-svc -P 9030 -u root --password="" \
                --connect-timeout=5 -e "SELECT 1" >/dev/null 2>&1 || \
                mysql -h starrocks-fe-svc -P 9030 -u root \
                --password="${SR_PASSWORD}" \
                --connect-timeout=5 -e "SELECT 1" >/dev/null 2>&1; do
                echo "FE not ready, retrying in 5s..."
                sleep 5
              done
              echo "✅ FE is up"

              mysql -h starrocks-fe-svc -P 9030 -u root --password="${SR_PASSWORD}" \
                --connect-timeout=30 <<SQL

              CREATE DATABASE IF NOT EXISTS fraud;

              USE fraud;

              -- ── Real-time transactions (Primary Key = upsert safe) ──
              DROP TABLE IF EXISTS transactions;
              CREATE TABLE IF NOT EXISTS transactions (
                transaction_id  VARCHAR(64)    NOT NULL,
                user_id         VARCHAR(64)    NOT NULL,
                amount          DECIMAL(18,2)  NOT NULL,
                merchant_id     VARCHAR(64)    NOT NULL,
                merchant_lat    DOUBLE,
                merchant_lon    DOUBLE,
                status          VARCHAR(20),
                event_time      DATETIME       NOT NULL,
                ingest_time     DATETIME       NOT NULL,
                plan            VARCHAR(20),
                velocity_5min   INT,
                amount_zscore   DOUBLE,
                geo_speed_kmh   DOUBLE,
                fraud_score     INT,
                is_flagged      BOOLEAN
              )
              PRIMARY KEY (transaction_id)
              DISTRIBUTED BY HASH(transaction_id) BUCKETS 12
              PROPERTIES ("replication_num" = "1");

              -- ── Fraud scores (queryable by dashboard) ──
              DROP TABLE IF EXISTS fraud_scores;
              CREATE TABLE IF NOT EXISTS fraud_scores (
                transaction_id  VARCHAR(64)  NOT NULL,
                user_id         VARCHAR(64)  NOT NULL,
                fraud_score     INT          NOT NULL,
                reasons         VARCHAR(500),
                flagged_at      DATETIME     NOT NULL,
                reviewed        BOOLEAN
              )
              PRIMARY KEY (transaction_id)
              DISTRIBUTED BY HASH(transaction_id) BUCKETS 6
              PROPERTIES ("replication_num" = "1");

              -- ── Risk profiles (per user, updated each batch by Spark) ──
              DROP TABLE IF EXISTS risk_profiles;
              CREATE TABLE IF NOT EXISTS risk_profiles (
                user_id            VARCHAR(64)   NOT NULL,
                avg_amount_30d     DECIMAL(18,2),
                stddev_amount      DECIMAL(18,2),
                avg_velocity_1h    DOUBLE,
                last_merchant_lat  DOUBLE,
                last_merchant_lon  DOUBLE,
                updated_at         DATETIME
              )
              PRIMARY KEY (user_id)
              DISTRIBUTED BY HASH(user_id) BUCKETS 6
              PROPERTIES ("replication_num" = "1");

              SQL

              echo "✅ StarRocks DDL complete"
EOF

kubectl apply -f $REPO_ROOT/gitops/bigdata/starrocks/init-ddl-job.yaml

kubectl -n bigdata wait --for=condition=complete \
  job/starrocks-init-ddl --timeout=300s

kubectl -n bigdata logs job/starrocks-init-ddl  

#Troubleshooting
Fix init-ddl-job.yaml
kubectl -n bigdata delete job starrocks-init-ddl
kubectl apply -f $REPO_ROOT/gitops/bigdata/starrocks/init-ddl-job.yaml
kubectl -n bigdata wait --for=condition=complete job/starrocks-init-ddl --timeout=300s
kubectl -n bigdata logs job/starrocks-init-ddl
```

### 2.4 Validate Phase 2

```bash
SR_PASS=$(kubectl -n bigdata get secret starrocks-credentials \
  -o jsonpath='{.data.root-password}' | base64 -d)

kubectl -n bigdata exec starrocks-fe-0 -- \
  mysql -h 127.0.0.1 -P 9030 -u root --password="${SR_PASS}" \
  -e "SHOW DATABASES;"

kubectl -n bigdata exec starrocks-fe-0 -- \
  mysql -h 127.0.0.1 -P 9030 -u root --password="${SR_PASS}" \
  -e "USE fraud; SHOW TABLES;"

unset SR_PASS
# Expect: transactions, fraud_scores, risk_profiles
```

### 2.5 Commit Phase 2 to GitHub

```bash
cd $REPO_ROOT
git add gitops/bigdata/starrocks/starrocks.yaml \
gitops/bigdata/starrocks/starrocks-init-job.yaml \
gitops/bigdata/starrocks/init-ddl-job.yaml

git commit -m "Phase 2: StarRocks StatefulSet + RBAC init job + fraud DDL (3 tables)"

git push origin main
```

---

## ════════════════════════════════════════
## PHASE 3 — APACHE ICEBERG CATALOG CONFIG
## ════════════════════════════════════════

> Iceberg sits on top of MinIO.
> FIX 12: JDBC catalog backed by Airflow's PostgreSQL for safe concurrent writes.
> Spark reads/writes Iceberg tables on MinIO directly.

### 3.1 Create Iceberg Catalog ConfigMap

```bash
mkdir -p $REPO_ROOT/gitops/bigdata/minio

# FIX 12: JDBC catalog (was hadoop — no locking on MinIO)
cat > $REPO_ROOT/gitops/bigdata/minio/iceberg-catalog-config.yaml <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: iceberg-catalog-config
  namespace: bigdata
data:
  # Spark will mount this and load it as --conf values
  spark-iceberg.conf: |
    spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
    spark.sql.catalog.iceberg.type=jdbc
    spark.sql.catalog.iceberg.uri=jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog
    spark.sql.catalog.iceberg.jdbc.user=postgres
    spark.sql.catalog.iceberg.warehouse=s3a://iceberg/warehouse
    spark.hadoop.fs.s3a.endpoint=http://minio.bigdata.svc.cluster.local:9000
    spark.hadoop.fs.s3a.path.style.access=true
    spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
    spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
    # Credentials NOT stored here — injected at runtime via:
    # spark.hadoop.fs.s3a.access.key = MINIO_ACCESS_KEY env var (from minio-secret Secret)
    # spark.hadoop.fs.s3a.secret.key = MINIO_SECRET_KEY env var (from minio-secret Secret)
    # spark.sql.catalog.iceberg.jdbc.password = ICEBERG_DB_PASSWORD env var
    # Set in SparkSession builder in fraud_stream_to_starrocks.py
EOF

kubectl apply -f $REPO_ROOT/gitops/bigdata/minio/iceberg-catalog-config.yaml
```

### 3.2 Create Enrichment ConfigMap

```bash
mkdir -p $REPO_ROOT/gitops/bigdata/enrichment

cat > $REPO_ROOT/gitops/bigdata/enrichment/customer-csv-configmap.yaml <<'EOF'
apiVersion: v1
kind: ConfigMap
metadata:
  name: customer-csv
  namespace: bigdata
data:
  customer.csv: |
    user_id,plan,avg_amount_30d,credit_limit
    user_000,PREMIUM,97.01,5000.00
    user_001,BASIC,209.48,2000.00
    user_002,PREPAID,138.14,500.00
    user_003,BASIC,161.5,2000.00
    user_004,PREMIUM,201.97,5000.00
EOF

kubectl apply -f $REPO_ROOT/gitops/bigdata/enrichment/customer-csv-configmap.yaml
```

### 3.3 — Create MinIO Buckets

```bash
cat > $REPO_ROOT/gitops/bigdata/minio/minio-buckets-job.yaml <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: minio-bootstrap
  namespace: bigdata
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: OnFailure
      containers:
        - name: mc
          image: quay.io/minio/mc:RELEASE.2024-06-12T14-34-03Z
          env:
            - name: MINIO_USER
              valueFrom:
                secretKeyRef:
                  name: minio-secret
                  key: MINIO_ROOT_USER
            - name: MINIO_PASS
              valueFrom:
                secretKeyRef:
                  name: minio-secret
                  key: MINIO_ROOT_PASSWORD
          command: ["/bin/bash", "-c"]
          args:
            - |
              set -euo pipefail
              mc alias set local http://minio:9000 "${MINIO_USER}" "${MINIO_PASS}"
              mc mb local/iceberg --ignore-existing
              mc mb local/checkpoints --ignore-existing
              mc ls local
EOF

kubectl apply -f $REPO_ROOT/gitops/bigdata/minio/minio-buckets-job.yaml

kubectl -n bigdata wait \
  --for=condition=complete \
  job/minio-bootstrap \
  --timeout=120s

kubectl -n bigdata logs job/minio-bootstrap

# check created buckets directly on MiniO
kubectl -n bigdata exec minio-0 -- ls /data
```

### 3.4 Validate Phase 3

```bash
kubectl -n bigdata get configmap iceberg-catalog-config -o yaml
kubectl -n bigdata get configmap customer-csv -o yaml

# FIX 11+13: Pinned mc tag, credentials via --env= (not kubectl inside pod)
kubectl -n bigdata run mc-verify --rm --attach --restart=Never \
  --image=quay.io/minio/mc:RELEASE.2024-06-12T14-34-03Z \
  --env="MINIO_USER=$(kubectl -n bigdata get secret minio-secret \
    -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)" \
  --env="MINIO_PASS=$(kubectl -n bigdata get secret minio-secret \
    -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)" \
  --command -- bash -c '
    set -euo pipefail
    mc alias set local http://minio:9000 "${MINIO_USER}" "${MINIO_PASS}"
    mc ls local/iceberg
    mc ls local/checkpoints
  '
```

### 3.5 Commit Phase 3 to GitHub

```bash
cd $REPO_ROOT
git add gitops/bigdata/minio/iceberg-catalog-config.yaml \
        gitops/bigdata/minio/minio-buckets-job.yaml \
        gitops/bigdata/enrichment/customer-csv-configmap.yaml
git commit -m "Phase 3: Iceberg JDBC catalog ConfigMap + customer enrichment CSV + MinIO buckets"
git push origin main
```

---

## ════════════════════════════════════════
## PHASE 4 — BUILDKIT PIPELINE
## ════════════════════════════════════════

> BuildKit replaces Kaniko (archived June 2025).
> Same .yaml.tpl + envsubst + GIT_SHA pattern as Cassandra lab.
> BuildKit daemon runs as a Deployment. Build Jobs connect to it.

### 4.1 Deploy BuildKit Daemon

```bash
mkdir -p $REPO_ROOT/gitops/bigdata/buildkit

cat > $REPO_ROOT/gitops/bigdata/buildkit/buildkitd.yaml <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: buildkitd
  namespace: bigdata
spec:
  replicas: 1
  selector:
    matchLabels:
      app: buildkitd
  template:
    metadata:
      labels:
        app: buildkitd
    spec:
      serviceAccountName: buildkit
      containers:
        - name: buildkitd
          image: moby/buildkit:v0.13.2
          args:
            - --addr
            - tcp://0.0.0.0:1234
            - --addr
            - unix:///run/buildkit/buildkitd.sock
          securityContext:
            privileged: true
          ports:
            - containerPort: 1234
          resources:
            requests:
              cpu: "250m"
              memory: "512Mi"
            limits:
              cpu: "2"
              memory: "4Gi"
          volumeMounts:
            - name: ghcr-secret
              mountPath: /root/.docker
              readOnly: true
      volumes:
        - name: ghcr-secret
          secret:
            secretName: ghcr-creds
            items:
              - key: .dockerconfigjson
                path: config.json
---
apiVersion: v1
kind: Service
metadata:
  name: buildkitd
  namespace: bigdata
spec:
  selector:
    app: buildkitd
  ports:
    - port: 1234
      targetPort: 1234
EOF

# The kubectl apply on a Deployment handles the rollout — it patches the existing Deployment and triggers a rolling update automatically. No need to delete the pod manually

kubectl apply -f $REPO_ROOT/gitops/bigdata/buildkit/buildkitd.yaml

# Rollout status for Deployment — this IS correct and prod style. Deployments use rollout status, StatefulSets use kubectl wait. 

kubectl -n bigdata rollout status deploy/buildkitd --timeout=180s
```

### 4.2 Spark Job Dockerfile

```bash
mkdir -p $REPO_ROOT/k8s-bigdata/spark-fraud-job

cat > $REPO_ROOT/k8s-bigdata/spark-fraud-job/Dockerfile <<'EOF'
FROM ghcr.io/jjcorderomejia/spark:3.5.6-20260207032726

USER root

# Install pip and Python dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir "pyarrow>=16.0.0,<18.0.0" pandas

# Copy Spark jobs
COPY fraud_stream_to_starrocks.py      /opt/spark/jobs/fraud_stream_to_starrocks.py
COPY customer_csv_to_iceberg.py        /opt/spark/jobs/customer_csv_to_iceberg.py
COPY init_iceberg_schema.py            /opt/spark/jobs/init_iceberg_schema.py
COPY compact_iceberg.py                /opt/spark/jobs/compact_iceberg.py
COPY risk_profiles_to_iceberg.py       /opt/spark/jobs/risk_profiles_to_iceberg.py

# StarRocks Spark connector JAR
ADD https://github.com/StarRocks/starrocks-connector-for-apache-spark/releases/download/v1.1.3/starrocks-spark-connector-3.5_2.12-1.1.3.jar \
    /opt/spark/jars/starrocks-spark-connector.jar

# Iceberg + S3A JARs
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.2/iceberg-spark-runtime-3.5_2.12-1.7.2.jar \
    /opt/spark/jars/iceberg-spark-runtime.jar

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    /opt/spark/jars/hadoop-aws.jar

ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    /opt/spark/jars/aws-java-sdk-bundle.jar

# PostgreSQL JDBC driver for Iceberg JDBC catalog
ADD https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    /opt/spark/jars/postgresql.jar

# Docker ADD from URL creates files as root:root 600 — make all JARs readable by hadoop (UID 1000)
RUN chmod 644 /opt/spark/jars/*.jar

# Switch back to the Spark service user defined in the base image.
# The base image (ghcr.io/jjcorderomejia/spark:3.5.6-*) uses 'hadoop' (UID 1000, GID 1000).
# Numeric UID required — runAsNonRoot: true in K8s cannot verify named users at admission.
# hadoop owns /opt/spark (755) — the entrypoint WORKDIR where java_opts.txt is written.
# IMPORTANT: The SparkApplication manifest (spark-fraud-stream.yaml.tpl) sets:
#   podSecurityContext.runAsNonRoot: true  — K8s enforces non-root at admission
#   podSecurityContext.fsGroup: 1000       — matches hadoop GID for volume mounts
# If the base image ever changes this UID, update fsGroup in spark-fraud-stream.yaml.tpl
# to match. The two values must stay in sync.
USER 1000
EOF
```



---

### 4.3 Commit 

```bash
cd $REPO_ROOT
git add gitops/bigdata/buildkit/buildkitd.yaml \
          k8s-bigdata/spark-fraud-job/Dockerfile
git commit -m "Phase 4: BuildKit daemon + Spark job Dockerfile + templates"
git push origin main
```

---

## ════════════════════════════════════════
## PHASE 4.4 — CUSTOMER CSV TO ICEBERG PY
## ════════════════════════════════════════


```python
mkdir -p $REPO_ROOT/spark-jobs

cat > $REPO_ROOT/spark-jobs/customer_csv_to_iceberg.py <<'PYEOF'
"""
Customer CSV → Iceberg Data Load Job

Reads customer enrichment CSV from ConfigMap mount and overwrites
iceberg.fraud.customers (pure data load — DDL owned by init_iceberg_schema.py).

Execution:
  - Initial deploy: K8s SparkApplication (customer-csv-to-iceberg.yaml.tpl)
  - Daily refresh: Airflow DAG (pending implementation)
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

CUSTOMER_CSV_PATH = os.environ.get("CUSTOMER_CSV_PATH", "/opt/enrichment/customer.csv")
MINIO_ACCESS_KEY  = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY  = os.environ["MINIO_SECRET_KEY"]
ICEBERG_DB_PASS   = os.environ["ICEBERG_DB_PASSWORD"]

spark = (
    SparkSession.builder
    .appName("customer-csv-to-iceberg")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── READ CUSTOMER CSV ─────────────────────────────────────────────────
customers = (
    spark.read
    .option("header", "true")
    .csv(CUSTOMER_CSV_PATH)
    .select(
        col("user_id"),
        col("plan"),
        col("avg_amount_30d").cast("double"),
        col("credit_limit").cast("double")
    )
)

# ── WRITE TO ICEBERG — overwrite (pure data load, DDL in init_iceberg_schema.py) ──
customers.writeTo("iceberg.fraud.customers").overwritePartitions()

print(f"✅ {customers.count()} customer records written to iceberg.fraud.customers")

spark.stop()
PYEOF
```

---


## ════════════════════════════════════════
## PHASE 4.44 — ICEBERG SCHEMA INIT JOB PY
## ════════════════════════════════════════


```bash
cat > $REPO_ROOT/spark-jobs/init_iceberg_schema.py <<'PYEOF'
"""
Iceberg Schema Init Job — run ONCE on initial deploy.

Owns ALL Iceberg DDL. Creates all four tables if they do not exist:
  - iceberg.fraud.customers         — customer enrichment reference
  - iceberg.fraud.transactions_lake — full audit trail (partitioned by event_time)
  - iceberg.fraud.processed_batches — streaming batch dedup guard
  - iceberg.fraud.risk_profiles     — risk profile snapshot, synced from StarRocks by Airflow

Run before any other Spark job. Safe to re-run (CREATE TABLE IF NOT EXISTS).
"""
import os
from pyspark.sql import SparkSession

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
ICEBERG_DB_PASS  = os.environ["ICEBERG_DB_PASSWORD"]

spark = (
    SparkSession.builder
    .appName("init-iceberg-schema")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── CREATE DATABASE IF NOT EXISTS ─────────────────────────────────────
spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.fraud")

# ── CUSTOMERS — customer enrichment reference ─────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.fraud.customers (
        user_id        STRING,
        plan           STRING,
        avg_amount_30d DOUBLE,
        credit_limit   DOUBLE
    )
    USING iceberg
""")
print("✅ iceberg.fraud.customers ready")

# ── TRANSACTIONS LAKE — full audit trail, partitioned by event_time ───
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.fraud.transactions_lake (
        transaction_id STRING,
        user_id        STRING,
        amount         DOUBLE,
        merchant_id    STRING,
        merchant_lat   DOUBLE,
        merchant_lon   DOUBLE,
        status         STRING,
        event_time     TIMESTAMP,
        ingest_time    TIMESTAMP,
        fraud_score    INT,
        is_flagged     BOOLEAN,
        reasons        STRING
    )
    USING iceberg
    PARTITIONED BY (days(event_time))
""")
print("✅ iceberg.fraud.transactions_lake ready")

# ── PROCESSED BATCHES — streaming batch dedup guard ───────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.fraud.processed_batches (
        batch_id     BIGINT NOT NULL,
        processed_at TIMESTAMP
    )
    USING iceberg
""")
print("✅ iceberg.fraud.processed_batches ready")

# ── RISK PROFILES — snapshot synced from StarRocks by Airflow ─────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.fraud.risk_profiles (
        user_id           STRING,
        avg_amount_30d    DOUBLE,
        stddev_amount     DOUBLE,
        avg_velocity_1h   DOUBLE,
        last_merchant_lat DOUBLE,
        last_merchant_lon DOUBLE,
        updated_at        TIMESTAMP
    )
    USING iceberg
""")
print("✅ iceberg.fraud.risk_profiles ready")

spark.stop()
PYEOF
```

---


## ════════════════════════════════════════
## PHASE 4.441 — RISK PROFILES TO ICEBERG SYNC JOB
## ════════════════════════════════════════

Reads `fraud.risk_profiles` from StarRocks (batch, standalone — no streaming concurrency) and
overwrites `iceberg.fraud.risk_profiles` on MinIO. Runs after `refresh_risk_profiles` Airflow task.
Enables `fraud_stream_to_starrocks.py` to read risk profiles from Iceberg (S3A/Parquet) instead
of the StarRocks Thrift scanner — eliminates executor SIGABRT (exit code 134) on streaming executors.

```bash
cat > $REPO_ROOT/spark-jobs/risk_profiles_to_iceberg.py <<'PYEOF'
"""
Risk Profiles → Iceberg Sync Job

Reads fraud.risk_profiles from StarRocks (batch, standalone — no streaming concurrency)
and overwrites iceberg.fraud.risk_profiles on MinIO/S3A.

Runs daily via Airflow SparkKubernetesOperator after refresh_risk_profiles task.
Streaming job reads from Iceberg — avoids AbstractStarrocksRDD Thrift scanner on
streaming executors which causes SIGABRT (exit code 134).
"""
import os
from pyspark.sql import SparkSession

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
ICEBERG_DB_PASS  = os.environ["ICEBERG_DB_PASSWORD"]
SR_HOST          = os.environ.get("STARROCKS_FE_HOST", "starrocks-fe-svc")
SR_USER          = os.environ.get("STARROCKS_USER", "root")
SR_PASSWORD      = os.environ.get("STARROCKS_PASSWORD", "")
SR_DB            = os.environ.get("STARROCKS_DB", "fraud")

spark = (
    SparkSession.builder
    .appName("risk-profiles-to-iceberg")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "jdbc")
    .config("spark.sql.catalog.iceberg.uri",
            "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog")
    .config("spark.sql.catalog.iceberg.jdbc.user", "postgres")
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint",
            "http://minio.bigdata.svc.cluster.local:9000")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

risk_profiles = (
    spark.read
    .format("starrocks")
    .option("starrocks.fenodes", f"{SR_HOST}:8030")
    .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{SR_HOST}:9030")
    .option("starrocks.table.identifier", f"{SR_DB}.risk_profiles")
    .option("starrocks.user", SR_USER)
    .option("starrocks.password", SR_PASSWORD)
    .load()
)

count = risk_profiles.count()
print(f"Read {count} risk profiles from StarRocks")

(
    risk_profiles.write
    .format("iceberg")
    .mode("overwrite")
    .saveAsTable("iceberg.fraud.risk_profiles")
)
print(f"✅ Wrote {count} risk profiles to iceberg.fraud.risk_profiles")

spark.stop()
PYEOF
```

```bash
cd $REPO_ROOT
git add spark-jobs/risk_profiles_to_iceberg.py
git commit -m "Phase 4.441: risk_profiles_to_iceberg.py"
git push origin main
```

---


## ════════════════════════════════════════
## PHASE 4.45 — ICEBERG SCHEMA INIT YAML
## ════════════════════════════════════════


```bash
cat > $REPO_ROOT/k8s-bigdata/spark-fraud-job/init-iceberg-schema.yaml.tpl <<'EOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: init-iceberg-schema
  namespace: bigdata
spec:
  type: Python
  mode: cluster
  sparkVersion: "3.5.6"
  image: ghcr.io/${ORG}/fraud-spark-job:${GIT_SHA}
  imagePullPolicy: Always
  imagePullSecrets:
    - ghcr-creds

  mainApplicationFile: local:///opt/spark/jobs/init_iceberg_schema.py

  sparkConf:
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.iceberg.type": "jdbc"
    "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog"
    "spark.sql.catalog.iceberg.jdbc.user": "postgres"
    "spark.sql.catalog.iceberg.warehouse": "s3a://iceberg/warehouse"
    "spark.hadoop.fs.s3a.endpoint": "http://minio.bigdata.svc.cluster.local:9000"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"

  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "1g"
    serviceAccount: spark
    env:
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password

  executor:
    instances: 1
    cores: 1
    memory: "1g"
    env:
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password

  restartPolicy:
    type: Never
EOF
```

---


### 4.46 Commit ICEBERG SCHEMA INIT JOB

```bash
cd $REPO_ROOT
git add spark-jobs/init_iceberg_schema.py
git add k8s-bigdata/spark-fraud-job/init-iceberg-schema.yaml.tpl
git commit -m "Phase 4: init_iceberg_schema.py owns all Iceberg DDL; customer CSV to pure data load"
git push origin main
```

---


## ════════════════════════════════════════
## PHASE 4.5 — CSV TO ICEBERG YAML
## ════════════════════════════════════════



```bash
cat > $REPO_ROOT/k8s-bigdata/spark-fraud-job/customer-csv-to-iceberg.yaml.tpl <<'EOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: customer-csv-to-iceberg
  namespace: bigdata
spec:
  type: Python
  mode: cluster
  sparkVersion: "3.5.6"
  image: ghcr.io/${ORG}/fraud-spark-job:${GIT_SHA}
  imagePullPolicy: Always
  imagePullSecrets:
    - ghcr-creds

  mainApplicationFile: local:///opt/spark/jobs/customer_csv_to_iceberg.py

  sparkConf:
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.iceberg.type": "jdbc"
    "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog"
    "spark.sql.catalog.iceberg.jdbc.user": "postgres"
    "spark.sql.catalog.iceberg.warehouse": "s3a://iceberg/warehouse"
    "spark.hadoop.fs.s3a.endpoint": "http://minio.bigdata.svc.cluster.local:9000"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"

  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "1g"
    serviceAccount: spark
    env:
      - name: CUSTOMER_CSV_PATH
        value: "/opt/enrichment/customer.csv"
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
    volumeMounts:
      - name: enrichment
        mountPath: /opt/enrichment

  executor:
    instances: 1
    cores: 1
    memory: "1g"
    env:
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
    volumeMounts:
      - name: enrichment
        mountPath: /opt/enrichment

  volumes:
    - name: enrichment
      configMap:
        name: customer-csv

  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 5
    onSubmissionFailureRetryInterval: 20
EOF
```

---

### 4.6 Commit CUSTOMER CSV TO ICEBERG PY / YAML

```bash
cd $REPO_ROOT
git add spark-jobs/customer_csv_to_iceberg.py \
          k8s-bigdata/spark-fraud-job/customer-csv-to-iceberg.yaml.tpl
git commit -m "Phase 4: CUSTOMER CSV TO ICEBERG PY / YAML"
git push origin main
```

---

## ════════════════════════════════════════
## PHASE 5 — SPARK FRAUD STREAMING JOB
## ════════════════════════════════════════

```mermaid
flowchart TD
    RP([Redpanda\ntxn-raw])
    CSV([customer.csv\nConfigMap])

    CSV -->|daily Airflow\nad-hoc pending| INIT[customer_csv_to_iceberg]
    INIT --> CUST([Iceberg/MinIO\niceberg.fraud.customers])

    RP -->|transactions-raw| P

    subgraph SPARK[Spark Structured Streaming]
        P[PARSE] --> V[VALIDATE]
        V -->|bad| D[DLQ]
        V -->|good| F[FEATURES\nvelocity · 5-min window]
        F --> S[PRE-SCORE\nvelocity only]
    end

    D --> R1([Redpanda\ntxn-dlq])

    subgraph BATCH[write_all_sinks — foreachBatch]
        CE[ENRICH\ncustomer Iceberg TTL] --> ZS[ZSCORE\namount anomaly]
        ZS --> G[GEO ENRICH\nrisk_profiles broadcast]
        G --> FS[FINAL SCORE\nvelocity + zscore + geo\n≥60 = flagged]
    end

    S --> CE
    CUST -->|TTL cache| CE

    FS --> R2([Redpanda\ntxn-clean])
    FS -->|flagged| R3([Redpanda\nfraud-alerts])
    FS --> SR1([StarRocks\nfraud.transactions])
    FS -->|flagged| SR2([StarRocks\nfraud.fraud_scores])
    FS --> SR3([StarRocks\nfraud.risk_profiles])
    FS --> ICE([Iceberg/MinIO\nfraud.txn_lake])
    FS --> PB([Iceberg/MinIO\nfraud.processed_batches])
```

### 5.1 PySpark Job: `fraud_stream_to_starrocks.py`
| Sink | Final Repository | Purpose |
|------|-----------------|---------|
| DLQ bad records | Redpanda `transactions-dlq` | Reprocessing / dead letter — malformed records held for investigation |
| Fraud alerts | Redpanda `fraud-alerts` | Real-time alerting — downstream consumers (notification, case mgmt) |
| Clean stream | Redpanda `transactions-clean` | Fan-out — other consumers can subscribe without re-reading raw topic |
| All valid transactions + fraud score | StarRocks `fraud.transactions` | Hot query layer — dashboard, low-latency lookups, mutable upserts |
| Flagged transactions only | StarRocks `fraud.fraud_scores` | Fraud case store — reviewable, queryable fraud decisions |
| User last known location | StarRocks `fraud.risk_profiles` | Geo feature store — updated each batch, feeds broadcast cache for geo speed
computation |
| Full audit trail (valid transactions) | Iceberg on MinIO `iceberg/warehouse` → `fraud.transactions_lake` | Immutable audit trail — compliance,
historical analytics, daily compaction |
| Processed batch IDs | Iceberg on MinIO `iceberg/warehouse` → `fraud.processed_batches` | Batch dedup — prevents accumulator double-counting on
foreachBatch retry |
| Customer enrichment reference | Iceberg on MinIO `iceberg/warehouse` → `fraud.customers` | Customer plan + avg spend — loaded daily by Airflow, TTL-cached in foreachBatch |


```python
mkdir -p $REPO_ROOT/spark-jobs

cat > $REPO_ROOT/spark-jobs/fraud_stream_to_starrocks.py <<'PYEOF'
"""
Real-Time Payment Fraud Detection
Redpanda → Spark Structured Streaming → StarRocks + Iceberg + DLQ

Architecture:
  - At-least-once delivery (Kafka semantics)
  - Idempotent sink (StarRocks Primary Key upserts)
  - Stateful velocity via flatMapGroupsWithState + ProcessingTimeTimeout (no watermark dependency)
  - Rule-based fraud scoring (velocity · zscore · geo)
  - DLQ for malformed records
  - Iceberg append for full audit trail (JDBC catalog — safe concurrent writes)
  - Customer enrichment from iceberg.fraud.customers (daily Airflow refresh, TTL-cached)
  - Batch dedup via iceberg.fraud.processed_batches — safe accumulator on retry
"""
import json
import logging
import os
import time
from math import radians, sin, cos, sqrt, atan2

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, current_timestamp, when,
    to_timestamp, expr, struct, to_json, udf,
    count, sum as spark_sum, broadcast
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, LongType, ArrayType
)
from pyspark.sql.streaming.state import GroupStateTimeout

# ── ENV ──────────────────────────────────────────────────────────────
BOOTSTRAP               = os.environ["REDPANDA_BOOTSTRAP"]
TOPIC_IN                = os.environ.get("TOPIC_IN",                "transactions-raw")
TOPIC_DLQ               = os.environ.get("TOPIC_DLQ",               "transactions-dlq")
TOPIC_CLEAN             = os.environ.get("TOPIC_CLEAN",             "transactions-clean")
TOPIC_ALERTS            = os.environ.get("TOPIC_ALERTS",            "fraud-alerts")
SR_HOST                 = os.environ.get("STARROCKS_HOST",           "starrocks-fe-svc.bigdata.svc.cluster.local")
SR_PORT                 = os.environ.get("STARROCKS_PORT",           "9030")
SR_DB                   = os.environ.get("STARROCKS_DB",             "fraud")
SR_USER                 = os.environ.get("STARROCKS_USER",           "root")
SR_PASSWORD             = os.environ["STARROCKS_PASSWORD"]           # required — no default, fails fast if missing
MINIO_ACCESS_KEY        = os.environ["MINIO_ACCESS_KEY"]             # required — injected from K8s Secret
MINIO_SECRET_KEY        = os.environ["MINIO_SECRET_KEY"]             # required — injected from K8s Secret
ICEBERG_DB_PASS         = os.environ["ICEBERG_DB_PASSWORD"]          # required — Postgres password for Iceberg JDBC catalog
CUSTOMER_ICEBERG_TABLE  = os.environ.get("CUSTOMER_ICEBERG_TABLE",   "iceberg.fraud.customers")
CUSTOMER_CACHE_TTL      = int(os.environ.get("CUSTOMER_CACHE_TTL_SEC", "3600"))  # 1 hour — aligns with daily Airflow refresh
STARTING_OFFSETS        = os.environ.get("STARTING_OFFSETS",         "earliest")  # safe default — no gap on checkpoint loss
CHECKPOINT_BASE         = os.environ.get("CHECKPOINT_LOCATION",      "s3a://checkpoints/fraud-stream")
TRIGGER_INTERVAL        = os.environ.get("SPARK_TRIGGER_INTERVAL",   "10 seconds")
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))  # cap batch size — prevent OOM on backlog

# ── FRAUD THRESHOLDS (overridable via env) ────────────────────────────
VELOCITY_THRESHOLD  = int(os.environ.get("FRAUD_VELOCITY_THRESHOLD",  "10"))
ZSCORE_THRESHOLD    = float(os.environ.get("FRAUD_ZSCORE_THRESHOLD",  "3.0"))
GEO_SPEED_THRESHOLD = float(os.environ.get("FRAUD_GEO_KMH_THRESHOLD", "500.0"))
FRAUD_SCORE_CUTOFF  = int(os.environ.get("FRAUD_SCORE_CUTOFF",        "60"))

# ── KAFKA SSL — Redpanda uses TLS on all listeners ─────────────────────
# CA cert mounted from secret fraud-redpanda-default-root-certificate.
_KAFKA_SSL: dict = {
    "kafka.security.protocol":                     "SSL",
    "kafka.ssl.truststore.location":               "/etc/redpanda-certs/ca.crt",
    "kafka.ssl.truststore.type":                   "PEM",
    "kafka.ssl.endpoint.identification.algorithm": "",
}

# ── SCHEMA ────────────────────────────────────────────────────────────
txn_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id",        StringType(), True),
    StructField("amount",         DoubleType(), True),
    StructField("merchant_id",    StringType(), True),
    StructField("merchant_lat",   DoubleType(), True),
    StructField("merchant_lon",   DoubleType(), True),
    StructField("status",         StringType(), True),
    StructField("timestamp",      StringType(), True),
])

# ── VELOCITY STATE ────────────────────────────────────────────────────
# Rolling 5-min transaction count per user maintained in Spark state store.
# ProcessingTimeTimeout evicts idle users after VELOCITY_STATE_TIMEOUT —
# ensures state is bounded even when a user stops transacting entirely.
VELOCITY_WINDOW_SEC    = 5 * 60   # 5-minute rolling window
VELOCITY_STATE_TIMEOUT = 10 * 60 * 1000  # 10-minute idle eviction in ms — wall-clock, not event-time

_VELOCITY_OUTPUT_SCHEMA = StructType([
    StructField("transaction_id", StringType(),    True),
    StructField("user_id",        StringType(),    True),
    StructField("amount",         DoubleType(),    True),
    StructField("merchant_id",    StringType(),    True),
    StructField("merchant_lat",   DoubleType(),    True),
    StructField("merchant_lon",   DoubleType(),    True),
    StructField("status",         StringType(),    True),
    StructField("event_time",     TimestampType(), True),
    StructField("ingest_time",    TimestampType(), True),
    StructField("velocity_5min",  LongType(),      True),
    StructField("raw_json",       StringType(),    True),
    StructField("k_topic",        StringType(),    True),
    StructField("k_partition",    LongType(),      True),
    StructField("k_offset",       LongType(),      True),
])

# State schema: single array field holding event-time epoch floats.
# Tuple access: state.get[0] → list[float]
_VELOCITY_STATE_SCHEMA = StructType([
    StructField("timestamps", ArrayType(DoubleType()), True),
])

def _velocity_state_fn(key, pdf_iter, state):
    """
    applyInPandasWithState function — rolling 5-min velocity per user.

    State: array of event_time epochs (float, seconds since epoch).
    Window cutoff uses max(event_time) in the batch — pure event-time
    basis, tolerant of ingestion lag (no processing-time / event-time
    mismatch that would deflate velocity on delayed data).

    On ProcessingTimeTimeout (user idle > VELOCITY_STATE_TIMEOUT):
    remove state — prevents unbounded state growth for inactive users.
    """
    import pandas as pd
    import time

    if state.hasTimedOut:
        state.remove()
        return

    # state.get is a tuple; index 0 is the timestamps array field
    prev_ts    = list(state.get[0]) if state.exists else []
    batch_pdfs = list(pdf_iter)  # consume iterator once — reused below

    # Collect event-time epochs from this micro-batch
    new_ts = []
    for pdf in batch_pdfs:
        for ts in pdf["event_time"].dropna():
            new_ts.append(pd.Timestamp(ts).timestamp())

    # Pure event-time cutoff: anchor to latest event seen, not wall clock.
    # Falls back to time.time() only on a timeout batch (new_ts is empty).
    all_seen = prev_ts + new_ts
    now      = max(all_seen) if all_seen else time.time()
    cutoff   = now - VELOCITY_WINDOW_SEC

    all_ts   = [t for t in all_seen if t >= cutoff]
    velocity = len(all_ts)

    state.update((all_ts,))
    state.setTimeoutDuration(VELOCITY_STATE_TIMEOUT)

    _out_cols = [
        "transaction_id", "user_id", "amount", "merchant_id",
        "merchant_lat", "merchant_lon", "status",
        "event_time", "ingest_time",
        "raw_json", "k_topic", "k_partition", "k_offset",
    ]
    for pdf in batch_pdfs:
        out = pdf[_out_cols].copy()
        out["velocity_5min"] = velocity
        yield out

# ── HAVERSINE UDF (geo distance in km) ───────────────────────────────
# Used by compute_geo inside write_all_sinks foreachBatch
def haversine(lat1, lon1, lat2, lon2):
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return 0.0
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))

# ── COMPUTE GEO SPEED UDF ─────────────────────────────────────────────
# Looks up last known location from risk_profiles broadcast cache
# Returns speed km/h between last and current merchant location
def compute_geo(user_id, curr_lat, curr_lon, event_time):
    profile = risk_bc.value.get(user_id)
    if not profile or profile.get("last_merchant_lat") is None:
        return 0.0
    dist = haversine(
        profile["last_merchant_lat"], profile["last_merchant_lon"],
        curr_lat, curr_lon
    )
    if profile.get("updated_at") and event_time:
        delta_hours = (event_time.timestamp() - profile["updated_at"].timestamp()) / 3600
        return dist / delta_hours if delta_hours > 0 else 0.0
    return 0.0

compute_geo_udf = udf(compute_geo, DoubleType())

# ── SPARK SESSION ─────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("fraud-stream-to-starrocks")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)  # MinIO credentials from K8s Secret
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)  # Iceberg JDBC catalog password from K8s Secret
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── RISK PROFILE BROADCAST CACHE ──────────────────────────────────────
# TTL-cached inside foreachBatch — refreshed sequentially before each micro-batch.
# Sequential execution eliminates concurrent Spark job submission that caused
# executor SIGABRT (exit code 134) with the former background-thread pattern.
# On cold start cache is empty; geo score defaults to 0 (no false positives).
RISK_CACHE_TTL = int(os.environ.get("RISK_CACHE_TTL_SEC", "60"))
_risk_cache: dict = {"loaded_at": 0.0}

risk_bc = spark.sparkContext.broadcast({})

def refresh_risk_if_stale():
    global risk_bc
    if (time.time() - _risk_cache["loaded_at"]) < RISK_CACHE_TTL:
        return
    try:
        data = (
            spark.read
            .format("iceberg")
            .load("iceberg.fraud.risk_profiles")
            .rdd.map(lambda r: (r.user_id, r.asDict()))
            .collectAsMap()
        )
        risk_bc = spark.sparkContext.broadcast(data)
        _risk_cache["loaded_at"] = time.time()
    except Exception as e:
        logging.error(json.dumps({"event": "risk_refresh_failed", "error": str(e)}))

# ── CUSTOMER ENRICHMENT CACHE ──────────────────────────────────────────
# TTL-based in-memory cache. Loaded from iceberg.fraud.customers on MinIO.
# Populated daily by Airflow (customer_csv_to_iceberg.py).
# Refreshes every CUSTOMER_CACHE_TTL seconds inside foreachBatch.
_customer_cache: dict = {"df": None, "loaded_at": 0.0}

def get_customers_df():
    """Returns customer enrichment DataFrame from Iceberg, refreshing if TTL expired.
    Falls back to empty schema DataFrame if table not yet populated (first-boot / Airflow
    hasn't run yet). Stream degrades gracefully — plan and avg_amount_30d will be null
    until the daily fraud_customer_refresh DAG seeds the data."""
    now = time.time()
    if _customer_cache["df"] is None or (now - _customer_cache["loaded_at"]) > CUSTOMER_CACHE_TTL:
        try:
            df = (
                spark.read
                .format("iceberg")
                .load(CUSTOMER_ICEBERG_TABLE)
                .select(
                    col("user_id").alias("c_user_id"),
                    col("plan"),
                    col("avg_amount_30d").cast("double")
                )
            )
            _ = df.schema  # force JVM analysis — raises AnalysisException here if table missing
        except Exception as e:
            if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "table or view" in str(e).lower():
                logging.warning(json.dumps({
                    "event": "customer_table_not_found",
                    "table": CUSTOMER_ICEBERG_TABLE,
                    "note": "using empty enrichment until Airflow seeds data"
                }))
                df = spark.createDataFrame(
                    [], "c_user_id STRING, plan STRING, avg_amount_30d DOUBLE"
                )
            else:
                raise
        _customer_cache["df"] = df
        _customer_cache["loaded_at"] = now
    return _customer_cache["df"]

# ── OBSERVABILITY ACCUMULATORS ─────────────────────────────────────────
# Driver-side counters visible in Spark UI.
# Guarded by iceberg.fraud.processed_batches — incremented only once per batch_id.
acc_total    = spark.sparkContext.accumulator(0)
acc_flagged  = spark.sparkContext.accumulator(0)
acc_geo_hits = spark.sparkContext.accumulator(0)

# ── READ FROM REDPANDA ─────────────────────────────────────────────────
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", TOPIC_IN)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)  # cap batch size — prevent OOM on backlog
    .options(**_KAFKA_SSL)
    .load()
)

raw_with_meta = raw.select(
    col("topic").cast("string").alias("k_topic"),
    col("partition").cast("int").alias("k_partition"),
    col("offset").cast("long").alias("k_offset"),
    col("value").cast("string").alias("raw_json"),
)

parsed = raw_with_meta.withColumn("j", from_json(col("raw_json"), txn_schema))

# ── VALIDATION ────────────────────────────────────────────────────────
expanded = (
    parsed
    .withColumn("transaction_id", col("j.transaction_id"))
    .withColumn("user_id",        col("j.user_id"))
    .withColumn("amount",         col("j.amount"))
    .withColumn("merchant_id",    col("j.merchant_id"))
    .withColumn("merchant_lat",   col("j.merchant_lat"))
    .withColumn("merchant_lon",   col("j.merchant_lon"))
    .withColumn("status",         col("j.status"))
    .withColumn("event_time",     to_timestamp(col("j.timestamp")))
    .withColumn("ingest_time",    current_timestamp())
)

dlq_reason = (
    when(col("j").isNull(),                                                lit("schema_parse_error"))
    .when(col("transaction_id").isNull() | (col("transaction_id") == ""), lit("missing_transaction_id"))
    .when(col("user_id").isNull()        | (col("user_id") == ""),        lit("missing_user_id"))
    .when(col("amount").isNull(),                                          lit("missing_amount"))
    .when(col("amount") <= 0,                                             lit("non_positive_amount"))
    .when(col("merchant_id").isNull()    | (col("merchant_id") == ""),    lit("missing_merchant_id"))
    .when(col("event_time").isNull(),                                      lit("bad_timestamp"))
    .otherwise(lit(None))
)

tagged = expanded.withColumn("dlq_reason", dlq_reason)
bad    = tagged.filter(col("dlq_reason").isNotNull())
good   = tagged.filter(col("dlq_reason").isNull()).drop("dlq_reason", "j")

# ── FEATURE ENGINEERING ───────────────────────────────────────────────
# Velocity: rolling 5-min count per user via applyInPandasWithState.
# ProcessingTimeTimeout evicts idle user state on wall-clock inactivity.
# Window cutoff anchored to max(event_time) — event-time correct, lag tolerant.
# Emits one enriched row per input record immediately each micro-batch.
# No watermark cascade, no stream-stream join buffering.
final_df = (
    good
    .groupBy("user_id")
    .applyInPandasWithState(
        func=_velocity_state_fn,
        outputStructType=_VELOCITY_OUTPUT_SCHEMA,
        stateStructType=_VELOCITY_STATE_SCHEMA,
        outputMode="append",
        timeoutConf=GroupStateTimeout.ProcessingTimeTimeout,
    )
)

# ── WRITE ALL SINKS ───────────────────────────────────────────────────
def write_all_sinks(batch_df, batch_id: int):
    global acc_total, acc_flagged, acc_geo_hits
    if batch_df.isEmpty():
        return

    refresh_risk_if_stale()

    # ── VELOCITY SCORE ────────────────────────────────────────────
    # velocity_5min arrives from _velocity_state_fn (flatMapGroupsWithState).
    # Score computed here to keep the streaming plan free of Column expressions.
    batch_df = batch_df.withColumn("score_velocity",
        when(col("velocity_5min") > VELOCITY_THRESHOLD, lit(40)).otherwise(lit(0)))

    # ── CUSTOMER ENRICHMENT ───────────────────────────────────────
    # TTL-cached Iceberg read — reloads at most every CUSTOMER_CACHE_TTL seconds.
    # Populated daily by Airflow (customer_csv_to_iceberg.py).
    customers_df = get_customers_df()
    batch_df = (
        batch_df
        .join(broadcast(customers_df), batch_df.user_id == customers_df.c_user_id, "left")
        .drop("c_user_id")
        .withColumn("plan",
            when(col("plan").isNull(), lit("unknown")).otherwise(col("plan")))
        .withColumn("avg_amount_30d",
            when(col("avg_amount_30d").isNull(), lit(0.0)).otherwise(col("avg_amount_30d")))
    )

    # ── ZSCORE ────────────────────────────────────────────────────
    # Simplified stddev = 30% of mean — known limitation, pending business calibration
    batch_df = (
        batch_df
        .withColumn("amount_zscore",
            when(col("avg_amount_30d") > 0,
                (col("amount") - col("avg_amount_30d")) / (col("avg_amount_30d") * 0.3))
            .otherwise(lit(0.0)))
        .withColumn("score_zscore",
            when(col("amount_zscore") > ZSCORE_THRESHOLD, lit(30)).otherwise(lit(0)))
    )

    # ── GEO ENRICHMENT ────────────────────────────────────────────
    # Computes geo speed km/h using risk_profiles broadcast cache
    batch_df = (
        batch_df
        .withColumn("geo_speed_kmh",
            compute_geo_udf(
                col("user_id"), col("merchant_lat"),
                col("merchant_lon"), col("event_time")
            )
        )
        .withColumn("score_geo",
            when(col("geo_speed_kmh") > GEO_SPEED_THRESHOLD, lit(30)).otherwise(lit(0)))
    )

    # ── FINAL SCORE ───────────────────────────────────────────────
    batch_df = (
        batch_df
        .withColumn("fraud_score",
            col("score_velocity") + col("score_zscore") + col("score_geo"))
        .withColumn("is_flagged", col("fraud_score") >= FRAUD_SCORE_CUTOFF)
        .withColumn("reasons", expr("""
            concat_ws(',',
              case when score_velocity > 0 then 'HIGH_VELOCITY' end,
              case when score_zscore   > 0 then 'AMOUNT_ANOMALY' end,
              case when score_geo      > 0 then 'GEO_IMPOSSIBLE' end
            )
        """))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    # ── OBSERVABILITY — single aggregation pass ────────────────────
    stats = batch_df.agg(
        count("*").alias("total"),
        spark_sum(when(col("is_flagged"), lit(1)).otherwise(lit(0))).alias("flagged"),
        spark_sum(when(col("score_geo") > 0, lit(1)).otherwise(lit(0))).alias("geo_hits")
    ).collect()[0]

    total    = stats["total"]
    flagged  = stats["flagged"] or 0
    geo_hits = stats["geo_hits"] or 0

    # ── BATCH DEDUP — guard accumulators against retry double-count ─
    # iceberg.fraud.processed_batches is source of truth for seen batch_ids
    try:
        already_processed = (
            spark.read.format("iceberg").load("iceberg.fraud.processed_batches")
            .filter(col("batch_id") == batch_id)
            .count() > 0
        )
    except Exception:
        already_processed = False  # table doesn't exist on first run

    if not already_processed:
        acc_total    += total
        acc_flagged  += flagged
        acc_geo_hits += geo_hits

    try:
        # ── STARROCKS — transactions ──────────────────────────────
        try:
            (
                batch_df.select(
                    "transaction_id", "user_id", "amount", "merchant_id",
                    "merchant_lat", "merchant_lon", "status",
                    "event_time", "ingest_time", "plan",
                    "velocity_5min", "amount_zscore", "geo_speed_kmh",
                    "fraud_score", "is_flagged"
                )
                .write.format("starrocks")
                .option("starrocks.fenodes", f"{SR_HOST}:8030")
                .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{SR_HOST}:9030")
                .option("starrocks.table.identifier", f"{SR_DB}.transactions")
                .option("starrocks.user", SR_USER)
                .option("starrocks.password", SR_PASSWORD)
                .mode("append").save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "sr_transactions", "batch_id": batch_id, "error": str(e)}))

        # ── STARROCKS — fraud_scores ──────────────────────────────
        try:
            (
                batch_df.filter(col("is_flagged"))
                .select("transaction_id", "user_id", "fraud_score", "reasons",
                        col("ingest_time").alias("flagged_at"),
                        lit(None).cast("boolean").alias("reviewed"))
                .write.format("starrocks")
                .option("starrocks.fenodes", f"{SR_HOST}:8030")
                .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{SR_HOST}:9030")
                .option("starrocks.table.identifier", f"{SR_DB}.fraud_scores")
                .option("starrocks.user", SR_USER)
                .option("starrocks.password", SR_PASSWORD)
                .mode("append").save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "sr_fraud_scores", "batch_id": batch_id, "error": str(e)}))

        # ── STARROCKS — risk_profiles (latest location per user) ──
        try:
            (
                batch_df
                .withColumn("rn", expr(
                    "row_number() over (partition by user_id order by event_time desc)"
                ))
                .filter(col("rn") == 1)
                .select(
                    "user_id",
                    lit(None).cast("double").alias("avg_amount_30d"),
                    lit(None).cast("double").alias("stddev_amount"),
                    lit(None).cast("double").alias("avg_velocity_1h"),
                    col("merchant_lat").alias("last_merchant_lat"),
                    col("merchant_lon").alias("last_merchant_lon"),
                    col("event_time").alias("updated_at")
                )
                .write.format("starrocks")
                .option("starrocks.fenodes", f"{SR_HOST}:8030")
                .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{SR_HOST}:9030")
                .option("starrocks.table.identifier", f"{SR_DB}.risk_profiles")
                .option("starrocks.user", SR_USER)
                .option("starrocks.password", SR_PASSWORD)
                .mode("append").save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "sr_risk_profiles", "batch_id": batch_id, "error": str(e)}))

        # ── ICEBERG — audit trail ─────────────────────────────────
        try:
            (
                batch_df.select(
                    "transaction_id", "user_id", "amount", "merchant_id",
                    "merchant_lat", "merchant_lon", "status", "event_time",
                    "ingest_time", "fraud_score", "is_flagged", "reasons"
                )
                .write.format("iceberg")
                .mode("append")
                .save("iceberg.fraud.transactions_lake")
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "iceberg_txn_lake", "batch_id": batch_id, "error": str(e)}))

        # ── KAFKA — fraud alerts (keyed by user_id for ordering) ──
        try:
            (
                batch_df.filter(col("is_flagged"))
                .select(
                    col("user_id").cast("string").alias("key"),
                    to_json(struct(
                        col("transaction_id"), col("user_id"), col("amount"),
                        col("fraud_score"), col("reasons"),
                        col("ingest_time").cast("string").alias("flagged_at")
                    )).alias("value")
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP)
                .option("topic", TOPIC_ALERTS)
                .options(**_KAFKA_SSL)
                .save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "kafka_alerts", "batch_id": batch_id, "error": str(e)}))

        # ── KAFKA — clean stream (keyed by user_id for ordering) ──
        try:
            (
                batch_df.select(
                    col("user_id").cast("string").alias("key"),
                    to_json(struct(
                        "transaction_id", "user_id", "amount", "merchant_id",
                        "status", "event_time", "plan", "fraud_score", "is_flagged"
                    )).alias("value")
                )
                .write.format("kafka")
                .option("kafka.bootstrap.servers", BOOTSTRAP)
                .option("topic", TOPIC_CLEAN)
                .options(**_KAFKA_SSL)
                .save()
            )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "kafka_clean", "batch_id": batch_id, "error": str(e)}))

        # ── ICEBERG — processed batch dedup record ────────────────
        try:
            if not already_processed:
                (
                    spark.createDataFrame([(batch_id,)], ["batch_id"])
                    .withColumn("processed_at", current_timestamp())
                    .write.format("iceberg")
                    .mode("append")
                    .save("iceberg.fraud.processed_batches")
                )
        except Exception as e:
            logging.error(json.dumps({"event": "sink_failed", "sink": "iceberg_processed_batches", "batch_id": batch_id, "error": str(e)}))

        # ── STRUCTURED LOG ────────────────────────────────────────
        logging.warning(json.dumps({
            "batch_id":      batch_id,
            "total":         total,
            "flagged":       flagged,
            "geo_hits":      geo_hits,
            "acc_total":     acc_total.value,
            "acc_flagged":   acc_flagged.value,
            "acc_geo_hits":  acc_geo_hits.value
        }))

    finally:
        batch_df.unpersist()

# ── DLQ SINK ──────────────────────────────────────────────────────────
dlq_out = bad.select(
    to_json(struct(
        col("dlq_reason").alias("reason"),
        col("k_topic").alias("topic"),
        col("k_partition").alias("partition"),
        col("k_offset").alias("offset"),
        current_timestamp().cast("string").alias("ingest_time"),
        col("raw_json")
    )).alias("value")
)

dlq_query = (
    dlq_out
    .select(col("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("topic", TOPIC_DLQ)
    .options(**_KAFKA_SSL)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/dlq")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

# ── MAIN SINK ─────────────────────────────────────────────────────────
main_query = (
    final_df.writeStream
    .foreachBatch(write_all_sinks)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/main")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .start()
)

spark.streams.awaitAnyTermination()
PYEOF
```

---

### 5.2 Commit fraud_stream_to_starrocks.py

```bash
cd $REPO_ROOT
git add spark-jobs/fraud_stream_to_starrocks.py
git commit -m "Phase 5: fraud_stream_to_starrocks.py"
git push origin main
```

### 5.3 Iceberg Compaction Job: `compact_iceberg.py`

```python
cat > $REPO_ROOT/spark-jobs/compact_iceberg.py <<'PYEOF'
"""
Iceberg Compaction Job
Merges small Parquet files into 128MB targets.
Expires old snapshots (retain last 7).
Runs daily via Airflow SparkKubernetesOperator (Spark Operator cluster mode).
"""
import logging
import os

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
ICEBERG_DB_PASS  = os.environ["ICEBERG_DB_PASSWORD"]

TABLES = [
    "iceberg.fraud.transactions_lake",
    "iceberg.fraud.processed_batches",
]

spark = (
    SparkSession.builder
    .appName("iceberg-compaction")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "jdbc")
    .config("spark.sql.catalog.iceberg.uri",
            "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog")
    .config("spark.sql.catalog.iceberg.jdbc.user", "postgres")
    .config("spark.sql.catalog.iceberg.jdbc.password", ICEBERG_DB_PASS)
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://iceberg/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint",
            "http://minio.bigdata.svc.cluster.local:9000")
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

try:
    for table in TABLES:
        spark.sql(f"""
            CALL iceberg.system.rewrite_data_files(
                table => '{table}',
                strategy => 'binpack',
                options => map('target-file-size-bytes', '134217728')
            )
        """)
        log.info("%s compacted", table)

        spark.sql(f"""
            CALL iceberg.system.expire_snapshots(
                table => '{table}',
                retain_last => 7
            )
        """)
        log.info("%s snapshots expired (retained last 7)", table)

    log.info("Iceberg compaction complete")
finally:
    spark.stop()
PYEOF
```

### 5.4

```bash
# Copy PySpark job into build context
cp $REPO_ROOT/spark-jobs/fraud_stream_to_starrocks.py \
   $REPO_ROOT/k8s-bigdata/spark-fraud-job/

# Copy PySpark job into build context
cp $REPO_ROOT/spark-jobs/customer_csv_to_iceberg.py \
   $REPO_ROOT/k8s-bigdata/spark-fraud-job/   

# Copy Iceberg schema init job into build context
cp $REPO_ROOT/spark-jobs/init_iceberg_schema.py \
   $REPO_ROOT/k8s-bigdata/spark-fraud-job/

# Copy Iceberg compaction job into build context
cp $REPO_ROOT/spark-jobs/compact_iceberg.py \
   $REPO_ROOT/k8s-bigdata/spark-fraud-job/

# Copy risk profiles sync job into build context
cp $REPO_ROOT/spark-jobs/risk_profiles_to_iceberg.py \
   $REPO_ROOT/k8s-bigdata/spark-fraud-job/
```

---

### 5.5 Commit spark-jobs/compact_iceberg.py

```bash
# The following build context copies are NOT committed — they are generated
# artifacts created by the cp commands in 5.1c before every build:
#   k8s-bigdata/spark-fraud-job/compact_iceberg.py
#   k8s-bigdata/spark-fraud-job/customer_csv_to_iceberg.py
#   k8s-bigdata/spark-fraud-job/fraud_stream_to_starrocks.py
#   k8s-bigdata/spark-fraud-job/init_iceberg_schema.py
# spark-jobs/ is the source of truth.

cd $REPO_ROOT
git add spark-jobs/compact_iceberg.py
git commit -m "Phase 5: add compact_iceberg job"
git push origin main
```

---

### 5.6 BuildKit Job Template

```bash
# 4.3 only creates a local file (buildkit-job.yaml.tpl) via cat >. Nothing is applied to the cluster — it's a template that gets rendered and  applied later in 5.1d. No kubectl commands needed

cat > $REPO_ROOT/k8s-bigdata/spark-fraud-job/buildkit-job.yaml.tpl <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: buildkit-fraud-spark-${GIT_SHA}
  namespace: bigdata
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: buildkit
      imagePullSecrets:
        - name: ghcr-creds
      containers:
        - name: buildctl
          image: moby/buildkit:v0.13.2
          command:
            - buildctl
            - --addr
            - tcp://buildkitd.bigdata.svc.cluster.local:1234
            - build
            - --frontend=dockerfile.v0
            - --local
            - context=/workspace
            - --local
            - dockerfile=/workspace
            - --output
            - type=image,name=ghcr.io/${ORG}/fraud-spark-job:${GIT_SHA},push=true
          env:
            - name: DOCKER_CONFIG
              value: /kaniko/.docker
          volumeMounts:
            - name: workspace
              mountPath: /workspace
            - name: ghcr-secret
              mountPath: /kaniko/.docker
              readOnly: true
      initContainers:
        - name: copy-context
          image: busybox:1.36
          command: ['cp', '-r', '/src/.', '/workspace/']
          volumeMounts:
            - name: src
              mountPath: /src
            - name: workspace
              mountPath: /workspace
      volumes:
        - name: workspace
          emptyDir: {}
        - name: src
          hostPath:
            path: ${HOST_SPARK_JOB_ROOT}
            type: Directory
        - name: ghcr-secret
          secret:
            secretName: ghcr-creds
            items:
              - key: .dockerconfigjson
                path: config.json
EOF
```

### 5.7 Spark Application Template

```bash
# won't be applied until In 5.2 — rendered via envsubst and applied with kubectl apply to deploy the SparkApplication to the cluster.

cat > $REPO_ROOT/k8s-bigdata/spark-fraud-job/spark-fraud-stream.yaml.tpl <<'EOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: spark-fraud-stream
  namespace: bigdata
spec:
  type: Python
  mode: cluster
  sparkVersion: "3.5.6"
  image: ghcr.io/${ORG}/fraud-spark-job:${GIT_SHA}
  imagePullPolicy: Always
  imagePullSecrets:
    - ghcr-creds

  mainApplicationFile: local:///opt/spark/jobs/fraud_stream_to_starrocks.py

  sparkConf:
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.iceberg.type": "jdbc"
    "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog"
    "spark.sql.catalog.iceberg.jdbc.user": "postgres"
    "spark.sql.catalog.iceberg.warehouse": "s3a://iceberg/warehouse"
    "spark.hadoop.fs.s3a.endpoint": "http://minio.bigdata.svc.cluster.local:9000"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.sql.shuffle.partitions": "48"
    # Redpanda compatibility: force consumer-based offset fetching.
    # Spark 3.4+ KafkaOffsetReaderAdmin uses AdminClient.describeTopics which
    # times out against Redpanda — the deprecated consumer path works correctly.
    "spark.sql.streaming.kafka.useDeprecatedOffsetFetching": "true"

  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "2g"
    serviceAccount: spark
    # runAsNonRoot enforces non-root at K8s admission without pinning the UID.
    # fsGroup: 1000 matches hadoop GID in the base image (ghcr.io/jjcorderomejia/spark:3.5.6-*).
    # If the base image ever changes this GID, update fsGroup here AND in the executor section below.
    podSecurityContext:
      runAsNonRoot: true
      fsGroup: 1000
    env:
      - name: REDPANDA_BOOTSTRAP
        value: "fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092"
      - name: TOPIC_IN
        value: "transactions-raw"
      - name: TOPIC_DLQ
        value: "transactions-dlq"
      - name: TOPIC_CLEAN
        value: "transactions-clean"
      - name: TOPIC_ALERTS
        value: "fraud-alerts"
      - name: STARROCKS_HOST
        value: "starrocks-fe-svc.bigdata.svc.cluster.local"
      - name: STARROCKS_PORT
        value: "9030"
      - name: STARROCKS_DB
        value: "fraud"
      - name: STARROCKS_USER
        value: "root"
      - name: STARROCKS_PASSWORD
        valueFrom:
          secretKeyRef:
            name: starrocks-credentials
            key: root-password
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
      - name: CUSTOMER_ICEBERG_TABLE
        value: "iceberg.fraud.customers"
      - name: CUSTOMER_CACHE_TTL_SEC
        value: "3600"
      - name: STARTING_OFFSETS
        value: "earliest"
      - name: MAX_OFFSETS_PER_TRIGGER
        value: "50000"
      - name: CHECKPOINT_LOCATION
        value: "s3a://checkpoints/fraud-stream-v2"
    volumeMounts:
      - name: redpanda-certs
        mountPath: /etc/redpanda-certs
        readOnly: true

  executor:
    instances: 2
    cores: 2
    memory: "8g"
    memoryOverhead: "1g"
    # fsGroup must match driver — both use hadoop GID 1000 from the same base image.
    podSecurityContext:
      runAsNonRoot: true
      fsGroup: 1000
    env:
      - name: REDPANDA_BOOTSTRAP
        value: "fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092"
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: STARROCKS_PASSWORD
        valueFrom:
          secretKeyRef:
            name: starrocks-credentials
            key: root-password
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
    volumeMounts:
      - name: redpanda-certs
        mountPath: /etc/redpanda-certs
        readOnly: true

  volumes:
    - name: redpanda-certs
      secret:
        secretName: fraud-redpanda-default-root-certificate
        items:
          - key: ca.crt
            path: ca.crt

  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 5
    onSubmissionFailureRetryInterval: 20
EOF
```

### 5.8 Commit before build — SHA integrity

> ### SHA Lifecycle — How a git commit becomes a running pod
>
> **THE CHAIN**
>
> Every link must carry the same SHA. If any link has a different SHA,
> the traceability chain is broken.
>
> ```
> git commit → HEAD → $GIT_SHA → envsubst → rendered YAML → kubectl apply → K8s → GHCR → running pod
>    abc1234   abc1234  abc1234    abc1234      abc1234        abc1234      abc1234  abc1234
> ```
>
> **STEP BY STEP (correct — commit first)**
>
> 1. You commit your code changes:
>
>    `git commit -m "Fix: Redpanda TLS..."`
>
>    Git creates SHA abc1234. HEAD now points to abc1234.
>
> 2. The build script captures HEAD into a shell variable:
>
>    `export GIT_SHA=$(git rev-parse --short HEAD)`
>
>    $GIT_SHA is now abc1234.
>
> 3. BuildKit builds the Docker image and tags it with $GIT_SHA:
>
>    image pushed to GHCR as: `ghcr.io/jjcorderomejia/fraud-spark-job:abc1234`
>
>    The image contents = the code at abc1234. The image label = abc1234. They match.
>
> 4. envsubst renders the template. The template has a placeholder:
>
>    `image: ghcr.io/${ORG}/fraud-spark-job:${GIT_SHA}`
>
>    envsubst reads $GIT_SHA from the shell (abc1234), replaces the placeholder:
>
>    `image: ghcr.io/jjcorderomejia/fraud-spark-job:abc1234`
>
>    This goes into the rendered file: `_rendered/spark-fraud-stream.abc1234.yaml`
>
> 5. kubectl apply sends the rendered YAML to K8s.
>    K8s reads the image line, asks GHCR: "give me fraud-spark-job:abc1234".
>    GHCR says "yes, I have it", sends the image. K8s runs it as a pod.
>
>    Every link: abc1234. Git, image, manifest, running pod — all the same.
>
> **STEP BY STEP (broken — build before commit)**
>
> 1. You edit files on disk but do NOT commit yet.
>    HEAD is still the previous commit: 373abf3.
>
> 2. The build script captures HEAD:
>
>    `export GIT_SHA=$(git rev-parse --short HEAD)`
>
>    $GIT_SHA is 373abf3 — the OLD commit that does NOT contain your changes.
>
> 3. BuildKit builds the image from disk (which HAS your changes)
>    and tags it 373abf3:
>
>    image pushed to GHCR as: `ghcr.io/jjcorderomejia/fraud-spark-job:373abf3`
>
>    The image contents = new code with TLS fix.
>    The image label = 373abf3 = old code without TLS fix.
>    THEY DO NOT MATCH. This is a dirty build.
>
> 4. envsubst renders the template with $GIT_SHA=373abf3:
>
>    `image: ghcr.io/jjcorderomejia/fraud-spark-job:373abf3`
>
> 5. kubectl apply deploys it. K8s pulls 373abf3. The pod runs.
>
> 6. Now you commit:
>
>    `git commit -m "Fix: Redpanda TLS..."`
>
>    Git creates abc1234. But the running pod references 373abf3.
>
>    Result:
>    - git log says abc1234 is latest (contains TLS fix)
>    - K8s runs 373abf3 (also contains TLS fix — but the label says otherwise)
>    - git show 373abf3 shows OLD code WITHOUT the TLS fix
>    - An engineer debugging at 3am reads 373abf3 and sees the wrong code
>
>    The image label is a lie. The rendered manifest is a lie.
>    Everything says 373abf3 but nothing is actually 373abf3.
>
> References: Google SRE Book ch.8, SLSA v1.0, OpenGitOps, Skaffold tag strategies,
> Uber uBuild, NIST SP 800-204D.

```bash
cd $REPO_ROOT
git add   k8s-bigdata/spark-fraud-job/buildkit-job.yaml.tpl \
          k8s-bigdata/spark-fraud-job/spark-fraud-stream.yaml.tpl
git commit -m "Phase 5: BuildKit job template + SparkApplication template"
git push origin main
```

---

### 5.9 Build + Push Spark Job Image

> Builds and pushes the fraud Spark Docker image to GHCR from inside the cluster.
>
>  Steps:
>  1. Renders the BuildKit job manifest (substituting $ORG, $GIT_SHA, $HOST_SPARK_JOB_ROOT)
>  2. Sanity-checks the rendered image name before touching the cluster
>  3. Deletes any stale job with the same SHA (idempotent re-run)
>  4. Submits the BuildKit job to K8s — BuildKit daemon does the actual image build + push
>  5. Blocks until the job completes (or times out at 15min)
>  6. Tails logs so you immediately see build output / failures
>
>  End result: ghcr.io/$ORG/fraud-spark-job:$GIT_SHA is in the registry, ready for the SparkApplication to pull.

```bash

set -euo pipefail

export GIT_SHA=$(git -C $REPO_ROOT rev-parse --short HEAD)

export HOST_SPARK_JOB_ROOT=$REPO_ROOT/k8s-bigdata/spark-fraud-job
mkdir -p "$HOST_SPARK_JOB_ROOT/_rendered"

# Render BuildKit job
out_build="$HOST_SPARK_JOB_ROOT/_rendered/buildkit-fraud-spark.${GIT_SHA}.yaml"
envsubst < "$HOST_SPARK_JOB_ROOT/buildkit-job.yaml.tpl" > "$out_build"

# Sanity check image name
grep -nE 'name=ghcr' "$out_build"

# Run build
kubectl -n bigdata delete job buildkit-fraud-spark-${GIT_SHA} \
  --ignore-not-found || true
kubectl -n bigdata apply -f "$out_build"
kubectl -n bigdata wait \
  --for=condition=complete \
  job/buildkit-fraud-spark-${GIT_SHA} \
  --timeout=900s

kubectl -n bigdata logs job/buildkit-fraud-spark-${GIT_SHA} --tail=50

# End of Phase 5.1e, after successful build + push
echo "${GIT_SHA}" > $REPO_ROOT/k8s-bigdata/spark-fraud-job/_rendered/LAST_BUILT_SHA
```

---


### 5.10 Validate Phase 5

```bash
# SparkApplication running
kubectl -n bigdata get sparkapplication spark-fraud-stream

# StarRocks has data (after sending test events in Phase 8)
SR_PASS=$(kubectl -n bigdata get secret starrocks-credentials \
  -o jsonpath='{.data.root-password}' | base64 -d)
kubectl -n bigdata exec -it starrocks-fe-0 -- \
  mysql -h 127.0.0.1 -P 9030 -u root --password="${SR_PASS}" \
  -e "SELECT count(*) FROM fraud.transactions;"
unset SR_PASS
```



---

## ════════════════════════════════════════
## PHASE 5.50 — TRANSACTION GENERATOR
## ════════════════════════════════════════

> Continuous realistic transaction producer — keeps the dashboard live with fresh data at all times.
>
> **Why not a SparkApplication:**
> Spark is a distributed processing engine — it adds ~1GB memory overhead, a driver + executor pod,
> and JVM startup time. A transaction generator is a simple HTTP producer loop. Running it as a
> Spark job would be like using a freight train to deliver a letter. At FAANG/SRE scale you match
> the tool to the workload: Spark for distributed data processing, a lightweight Python container
> for a request loop.
>
> **Architecture:** Python 3.12-slim Deployment in `apps` namespace. POSTs to the FastAPI
> `/api/transaction` endpoint (same path a real payment client uses). Exponential backoff on
> failures (1s → 2s → 4s → ... → 60s cap). K8s `restartPolicy: Always` handles container crashes.
>
> **Repo layout:**
> ```
> k8s-apps/txn-generator/
> ├── generator.py                         # main script — HTTP producer loop
> ├── Dockerfile                           # Python 3.12-slim base
> ├── buildkit-job-txn-generator.yaml.tpl  # BuildKit job template
> └── txn-generator-deployment.yaml.tpl   # K8s Deployment template
> ```
>
> **Disk cost:** ~2/min × 2,880/day transactions → ~14MB/day Iceberg metadata peak.
> `compact_iceberg` at 02:00 daily resets to ~35KB (retain_last=7). Negligible steady-state.

---

### 5.50 generator.py

```bash
mkdir -p $REPO_ROOT/k8s-apps/txn-generator

cat > $REPO_ROOT/k8s-apps/txn-generator/generator.py <<'PYEOF'
"""
Realistic transaction generator — continuous HTTP producer for the Huanca fraud pipeline.

Produces to POST /api/transaction via the FastAPI backend.
Uses exponential backoff on failures: 1s, 2s, 4s, ... capped at 60s.
Runs indefinitely — deploy as a K8s Deployment with restartPolicy: Always.

Design decisions:
- 50 simulated users with realistic home lat/lon across US cities
- Poisson-distributed inter-arrival times (avg 2 txn/min) — realistic bursts
- 10% chance of fraud pattern per transaction:
    - velocity burst: 6 rapid transactions in 5 minutes
    - amount anomaly: 8–15x the user's typical spend
    - geo-speed violation: merchant location impossible to reach from last transaction
- All amounts, merchants, and locations are seeded per user for consistency
- X-Api-Key injected from API_KEY env var (matches backend-api secret)
"""

import os
import time
import uuid
import random
import math
import logging
import requests
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":%(message)s}'
)
log = logging.getLogger(__name__)

# ── CONFIG ─────────────────────────────────────────────────────────────
BACKEND_URL  = os.environ.get("BACKEND_URL",  "http://backend-api.apps.svc.cluster.local:8000")
API_KEY      = os.environ.get("API_KEY",      "")
TXN_RATE     = float(os.environ.get("TXN_RATE", "2.0"))   # avg transactions per minute
BACKOFF_MAX  = int(os.environ.get("BACKOFF_MAX", "60"))    # max backoff seconds

HEADERS = {"X-Api-Key": API_KEY, "Content-Type": "application/json"}

# ── SIMULATED USERS (50 users, home cities across US) ─────────────────
CITIES = [
    ("New York",      40.7128,  -74.0060),
    ("Los Angeles",   34.0522, -118.2437),
    ("Chicago",       41.8781,  -87.6298),
    ("Houston",       29.7604,  -95.3698),
    ("Phoenix",       33.4484, -112.0740),
    ("Philadelphia",  39.9526,  -75.1652),
    ("San Antonio",   29.4241,  -98.4936),
    ("San Diego",     32.7157, -117.1611),
    ("Dallas",        32.7767,  -96.7970),
    ("San Jose",      37.3382, -121.8863),
]

random.seed(42)
USERS = []
for i in range(50):
    city_name, city_lat, city_lon = CITIES[i % len(CITIES)]
    USERS.append({
        "user_id":      f"user_{i:03d}",
        "home_lat":     city_lat + random.uniform(-0.15, 0.15),
        "home_lon":     city_lon + random.uniform(-0.15, 0.15),
        "avg_spend":    round(random.uniform(20.0, 300.0), 2),
        "last_lat":     None,
        "last_lon":     None,
        "last_time":    None,
    })

MERCHANTS = [f"merchant_{i:04d}" for i in range(200)]

# ── HELPERS ────────────────────────────────────────────────────────────
def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def near(lat, lon, radius_deg=0.3):
    return (
        lat + random.uniform(-radius_deg, radius_deg),
        lon + random.uniform(-radius_deg, radius_deg),
    )

def far(lat, lon):
    """Return a location physically impossible to reach in < 5 min (geo-speed fraud)."""
    # Pick a random distant city — at least 1000km away
    candidates = [(c[1], c[2]) for c in CITIES if haversine_km(lat, lon, c[1], c[2]) > 1000]
    if not candidates:
        candidates = [(CITIES[0][1], CITIES[0][2])]
    base = random.choice(candidates)
    return near(base[0], base[1], 0.1)

def make_transaction(user: dict, fraud_type: str = None) -> dict:
    merchant_id  = random.choice(MERCHANTS)
    base_amount  = user["avg_spend"]

    if fraud_type == "amount":
        amount = round(base_amount * random.uniform(8.0, 15.0), 2)
        lat, lon = near(user["home_lat"], user["home_lon"])
    elif fraud_type == "geo" and user["last_lat"] is not None:
        amount = round(base_amount * random.uniform(0.5, 1.5), 2)
        lat, lon = far(user["last_lat"], user["last_lon"])
    else:
        amount = round(base_amount * random.uniform(0.3, 2.0), 2)
        lat, lon = near(user["home_lat"], user["home_lon"])

    return {
        "user_id":      user["user_id"],
        "amount":       max(1.0, amount),
        "merchant_id":  merchant_id,
        "merchant_lat": round(lat, 6),
        "merchant_lon": round(lon, 6),
        "status":       "pending",
    }

def post_transaction(payload: dict) -> bool:
    try:
        r = requests.post(
            f"{BACKEND_URL}/api/transaction",
            json=payload,
            headers=HEADERS,
            timeout=10,
        )
        if r.status_code == 202:
            log.info('"event":"txn_queued","user_id":"%s","amount":%.2f',
                     payload["user_id"], payload["amount"])
            return True
        else:
            log.warning('"event":"txn_rejected","status":%d,"body":"%s"',
                        r.status_code, r.text[:200])
            return False
    except Exception as e:
        log.error('"event":"txn_error","error":"%s"', str(e))
        return False

# ── MAIN LOOP ──────────────────────────────────────────────────────────
def main():
    backoff = 1
    log.info('"event":"generator_start","rate_per_min":%.1f,"users":%d', TXN_RATE, len(USERS))

    while True:
        user = random.choice(USERS)

        # 10% fraud pattern
        r = random.random()
        if r < 0.04:
            fraud_type = "geo"
        elif r < 0.10:
            fraud_type = "amount"
        elif r < 0.13:
            fraud_type = "velocity"
        else:
            fraud_type = None

        if fraud_type == "velocity":
            # Burst: 6 transactions in rapid succession for this user
            for _ in range(6):
                payload = make_transaction(user)
                ok = post_transaction(payload)
                if not ok:
                    time.sleep(min(backoff, BACKOFF_MAX))
                    backoff = min(backoff * 2, BACKOFF_MAX)
                else:
                    backoff = 1
                time.sleep(random.uniform(0.5, 2.0))
        else:
            payload = make_transaction(user, fraud_type)
            ok = post_transaction(payload)
            if not ok:
                log.warning('"event":"backoff","seconds":%d', backoff)
                time.sleep(min(backoff, BACKOFF_MAX))
                backoff = min(backoff * 2, BACKOFF_MAX)
                continue
            else:
                backoff = 1

        # Update user last location
        user["last_lat"]  = payload.get("merchant_lat")
        user["last_lon"]  = payload.get("merchant_lon")
        user["last_time"] = time.time()

        # Poisson inter-arrival: exponential distribution with mean = 60/TXN_RATE seconds
        sleep_s = random.expovariate(TXN_RATE / 60.0)
        time.sleep(sleep_s)

if __name__ == "__main__":
    main()
PYEOF
```

---

### 5.51 Dockerfile

```bash
cat > $REPO_ROOT/k8s-apps/txn-generator/Dockerfile <<'EOF'
FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir requests==2.31.0

COPY generator.py .

RUN useradd -m -u 1000 generator
USER generator

CMD ["python", "-u", "generator.py"]
EOF
```

---

### 5.52 BuildKit job template

```bash
cat > $REPO_ROOT/k8s-apps/txn-generator/buildkit-job-txn-generator.yaml.tpl <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: buildkit-txn-generator-${GIT_SHA}
  namespace: bigdata
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: buildkit
      imagePullSecrets:
        - name: ghcr-creds
      initContainers:
        - name: copy-context
          image: busybox:1.36
          command: ['cp', '-r', '/src/.', '/workspace/']
          volumeMounts:
            - name: src
              mountPath: /src
            - name: workspace
              mountPath: /workspace
      containers:
        - name: buildctl
          image: moby/buildkit:v0.13.2
          command:
            - buildctl
            - --addr
            - tcp://buildkitd.bigdata.svc.cluster.local:1234
            - build
            - --frontend=dockerfile.v0
            - --local
            - context=/workspace
            - --local
            - dockerfile=/workspace
            - --output
            - type=image,name=ghcr.io/${ORG}/txn-generator:${GIT_SHA},push=true
          volumeMounts:
            - name: workspace
              mountPath: /workspace
            - name: ghcr-secret
              mountPath: /kaniko/.docker
              readOnly: true
          env:
            - name: DOCKER_CONFIG
              value: /kaniko/.docker
      volumes:
        - name: workspace
          emptyDir: {}
        - name: src
          hostPath:
            path: ${HOST_TXN_GENERATOR_ROOT}
            type: Directory
        - name: ghcr-secret
          secret:
            secretName: ghcr-creds
            items:
              - key: .dockerconfigjson
                path: config.json
EOF
```

---

### 5.53 Deployment template

```bash
cat > $REPO_ROOT/k8s-apps/txn-generator/txn-generator-deployment.yaml.tpl <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: txn-generator
  namespace: apps
spec:
  replicas: 1
  selector:
    matchLabels:
      app: txn-generator
  template:
    metadata:
      labels:
        app: txn-generator
    spec:
      imagePullSecrets:
        - name: ghcr-creds
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
        - name: txn-generator
          image: ghcr.io/${ORG}/txn-generator:${GIT_SHA}
          imagePullPolicy: Always
          env:
            - name: BACKEND_URL
              value: "http://backend-api.apps.svc.cluster.local:8000"
            - name: API_KEY
              valueFrom:
                secretKeyRef:
                  name: api-key-secret
                  key: api-key
            - name: TXN_RATE
              value: "2.0"
            - name: BACKOFF_MAX
              value: "60"
          resources:
            requests:
              cpu: "50m"
              memory: "64Mi"
            limits:
              cpu: "200m"
              memory: "128Mi"
EOF
```

---

### 5.54 Commit

```bash
cd $REPO_ROOT
git add k8s-apps/txn-generator/generator.py
git add k8s-apps/txn-generator/Dockerfile
git add k8s-apps/txn-generator/buildkit-job-txn-generator.yaml.tpl
git add k8s-apps/txn-generator/txn-generator-deployment.yaml.tpl
git commit -m "feat: transaction generator — realistic Poisson producer, exponential backoff, fraud patterns"
git push origin main
```

---

### 5.55 Build + Deploy

```bash
source ~/.lab_Huanca
```

```bash
set -euo pipefail
export GIT_SHA=$(git -C $REPO_ROOT rev-parse --short HEAD)
export HOST_TXN_GENERATOR_ROOT=$REPO_ROOT/k8s-apps/txn-generator
mkdir -p "$HOST_TXN_GENERATOR_ROOT/_rendered"
```

```bash
out_build="$HOST_TXN_GENERATOR_ROOT/_rendered/buildkit-txn-generator.${GIT_SHA}.yaml"
envsubst < "$HOST_TXN_GENERATOR_ROOT/buildkit-job-txn-generator.yaml.tpl" > "$out_build"
```

```bash
kubectl -n bigdata delete job buildkit-txn-generator-${GIT_SHA} --ignore-not-found
```

```bash
kubectl -n bigdata apply -f "$out_build"
```

```bash
kubectl -n bigdata wait --for=condition=complete job/buildkit-txn-generator-${GIT_SHA} --timeout=900s
```

```bash
echo "${GIT_SHA}" > $HOST_TXN_GENERATOR_ROOT/_rendered/LAST_BUILT_SHA
```

```bash
out_deploy="$HOST_TXN_GENERATOR_ROOT/_rendered/txn-generator.${GIT_SHA}.yaml"
envsubst < "$HOST_TXN_GENERATOR_ROOT/txn-generator-deployment.yaml.tpl" > "$out_deploy"
```

```bash
kubectl -n apps apply -f "$out_deploy"
```

```bash
kubectl -n apps rollout status deploy/txn-generator --timeout=180s
```

```bash
# Validate image SHA
kubectl -n apps get deploy txn-generator \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

---

### 5.56 Validate

```bash
# Pod running
kubectl -n apps get pods | grep txn-generator
```

```bash
# Logs show transactions queued
kubectl -n apps logs deploy/txn-generator --tail=20
```

```bash
# StarRocks receiving data
SR_PASS=$(kubectl -n bigdata get secret starrocks-credentials -o jsonpath='{.data.root-password}' | base64 -d)
kubectl -n bigdata exec -i starrocks-fe-0 -- env MYSQL_PWD="${SR_PASS}" mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SELECT count(*), max(ingest_time) FROM fraud.transactions;"
```

---

## Spark Job Types — Reference

| Job | Type | Lifecycle | Airflow involved | Delivery |
|-----|------|-----------|-----------------|----------|
| `fraud_stream_to_starrocks.py` | Streaming | Permanent — runs until killed | No — deployed directly via `kubectl apply` | Built into image, SparkApplication manifest applied imperatively |
| `compact_iceberg.py` | Batch | Run-to-completion — daily | Yes — `SparkKubernetesOperator` triggers at 02:00 | Built into image, spec stored as Airflow Variable |
| `customer_csv_to_iceberg.py` | Batch | Run-to-completion — daily | Yes — `SparkKubernetesOperator` triggers at 02:00 | Built into image, spec stored as Airflow Variable |
| `risk_profiles_to_iceberg.py` | Batch | Run-to-completion — daily | Yes — `SparkKubernetesOperator` triggers after `refresh_risk_profiles` | Built into image, spec stored as Airflow Variable |
| `init_iceberg_schema.py` | Batch | Run-once — initial deploy only | No — deployed directly via `kubectl apply` | Built into image, SparkApplication manifest applied imperatively |

## How Spark knows: streaming vs batch

**The short answer:** the API that is called in the Python code determines the mode.
No external flag, no manifest setting — it is purely a code decision.

#### Batch jobs (`init_iceberg_schema.py`, `compact_iceberg.py`, `risk_profiles_to_iceberg.py`)

1. Read data with `spark.read` (static DataFrame)
2. Transform
3. Write result
4. Call `spark.stop()` — process exits with code 0

Spark Operator sees exit 0 → marks SparkApplication COMPLETED → pods terminate.
Airflow `SparkKubernetesOperator` unblocks → next task in chain runs.

Manifest uses `restartPolicy: Never` — one shot, fail fast, no retry.

#### Streaming job (`fraud_stream_to_starrocks.py`)

1. Read with `spark.readStream` (unbounded DataFrame)
2. Attach `foreachBatch` sink
3. Call `.start()` + `spark.streams.awaitAnyTermination()` — blocks forever

Process never exits → Spark Operator keeps the pod alive.
No Airflow involved — deployed once via `kubectl apply`, runs until killed or crashed.

Manifest uses `restartPolicy: Always` — Spark Operator restarts on crash automatically.

#### Decision rule

| If the job calls… | It is… | It exits? |
|---|---|---|
| `spark.readStream` + `awaitAnyTermination()` | Streaming | Never |
| `spark.read` + `spark.stop()` | Batch | Yes (code 0) |

---

## ════════════════════════════════════════
## PHASE 6 — AIRFLOW
## ════════════════════════════════════════

> Airflow K8sExecutor: each task runs as a K8s pod.
> No Celery, no Redis, no extra infra.
> DAGs: reconcile, feature refresh, compaction.
> All DAG tasks use KubernetesPodOperator — no kubectl, no subprocess.

### 6.1 Deploy Airflow

> Airflow deploys with these components:

> - Scheduler — parses DAGs, triggers tasks
> - API Server — UI (Airflow 3.x replaced webserver with api-server)
> - Triggerer — handles deferred tasks
> - KubernetesExecutor — each DAG task spawns its own K8s pod, dies when done. No workers sitting idle.
> - PostgreSQL (subchart) — Airflow's metadata DB AND reused as the Iceberg JDBC catalog backend

> The key architectural decision: reusing Airflow's PostgreSQL for the Iceberg catalog. Instead of running a separate Postgres instance just for
> Iceberg table metadata, the iceberg_catalog database is created inside the same Helm-managed PostgreSQL. One less service to operate.

> All components share the airflow ServiceAccount (created in Terraform Phase 0), which already has the RBAC needed to spawn K8s pods for task execution.

```bash
set -euo pipefail

# Add Airflow helm repo
helm repo add apache-airflow https://airflow.apache.org
helm repo update

mkdir -p $REPO_ROOT/gitops/bigdata/airflow

# Create Airflow admin credentials secret — auto-generated via openssl rand (base64 24 = 32 chars, cryptographically secure)
# Never typed by a human, never in shell history, never in bash_history — retrieve later via: kubectl -n bigdata get secret airflow-admin-credentials -o jsonpath='{.data.password}' | base64 -d
kubectl -n bigdata create secret generic airflow-admin-credentials \
  --from-literal=password="$(openssl rand -base64 24)" \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl -n bigdata create secret generic airflow-webserver-secret \
  --from-literal=webserver-secret-key="$(openssl rand -hex 32)" \
  --dry-run=client -o yaml | kubectl apply -f -

# Git-sync PAT — reuses GHCR PAT already stored in terraform-vars (has repo read scope)
kubectl -n bigdata create secret generic airflow-git-token-secret \
  --from-literal=GIT_SYNC_USERNAME=jjcorderomejia \
  --from-literal=GIT_SYNC_PASSWORD="$(kubectl -n bigdata get secret terraform-vars -o jsonpath='{.data.ghcr-token}' | base64 -d)" \
  --from-literal=GITSYNC_USERNAME=jjcorderomejia \
  --from-literal=GITSYNC_PASSWORD="$(kubectl -n bigdata get secret terraform-vars -o jsonpath='{.data.ghcr-token}' | base64 -d)" \
  --dry-run=client -o yaml | kubectl apply -f -

cat > $REPO_ROOT/gitops/bigdata/airflow/airflow-values.yaml <<'EOF'
executor: KubernetesExecutor

webserverSecretKeySecretName: airflow-webserver-secret

env:
  - name: AIRFLOW__CORE__LOAD_EXAMPLES
    value: "false"
  - name: AIRFLOW__WEBSERVER__EXPOSE_CONFIG
    value: "false"

extraEnv: |
  - name: MINIO_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: minio-secret
        key: MINIO_ROOT_USER
  - name: MINIO_SECRET_KEY
    valueFrom:
      secretKeyRef:
        name: minio-secret
        key: MINIO_ROOT_PASSWORD
  - name: STARROCKS_PASSWORD
    valueFrom:
      secretKeyRef:
        name: starrocks-credentials
        key: root-password

serviceAccount:
  name: airflow
  create: false

createUserJob:
  enabled: false   # admin created manually via Secret below — never default admin/admin

scheduler:
  serviceAccount:
    name: airflow
  extraVolumes:
    - name: spark-manifests
      configMap:
        name: compact-iceberg-spark-app
        optional: true
  extraVolumeMounts:
    - name: spark-manifests
      mountPath: /opt/spark-manifests
      readOnly: true

dagProcessor:
  serviceAccount:
    name: airflow
  extraVolumes:
    - name: spark-manifests
      configMap:
        name: compact-iceberg-spark-app
        optional: true
  extraVolumeMounts:
    - name: spark-manifests
      mountPath: /opt/spark-manifests
      readOnly: true

webserver:
  serviceAccount:
    name: airflow
  defaultUser:
    enabled: false   # disable default admin/admin — we create admin via Secret below

triggerer:
  serviceAccount:
    name: airflow

dags:
  gitSync:
    enabled: true
    repo: https://github.com/jjcorderomejia/Huanca.git
    branch: main
    ref: main
    subPath: dags
    credentialsSecret: airflow-git-token-secret
    period: 60s
    depth: 1

logs:
  persistence:
    enabled: true
    existingClaim: airflow-logs  # pre-created with ReadWriteOnce — local-path constraint

postgresql:
  enabled: true
  primary:
    persistence:
      storageClass: local-path
      size: 10Gi
EOF

# Pre-create airflow-logs PVC with ReadWriteOnce — local-path does not support ReadWriteMany
# Helm chart defaults to ReadWriteMany for logs; using existingClaim bypasses that constraint
kubectl -n bigdata apply -f - <<'PVCEOF'
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: airflow-logs
  namespace: bigdata
spec:
  accessModes:
    - ReadWriteOnce
  storageClassName: local-path
  resources:
    requests:
      storage: 10Gi
PVCEOF

# Verify airflow ServiceAccount exists — created by Terraform
# If missing, re-run "Run Terraform as a K8s Job" section in Phase 0 first
kubectl -n bigdata get serviceaccount airflow || \
  { echo "❌ airflow SA missing — re-run 'Run Terraform as a K8s Job' in Phase 0"; exit 1; }

# Helm cannot adopt the Terraform-created SA without these labels/annotations
kubectl -n bigdata annotate serviceaccount airflow \
  meta.helm.sh/release-name=airflow \
  meta.helm.sh/release-namespace=bigdata \
  --overwrite
kubectl -n bigdata label serviceaccount airflow \
  app.kubernetes.io/managed-by=Helm \
  --overwrite

helm upgrade --install airflow apache-airflow/airflow \
  --version 1.19.0 \
  --namespace bigdata \
  -f $REPO_ROOT/gitops/bigdata/airflow/airflow-values.yaml
# Note: --wait intentionally omitted — Helm post-install hook (migration job) deadlocks with --wait
# Monitor manually: kubectl -n bigdata get pods | grep airflow

kubectl -n bigdata get pods | grep airflow

# Create Airflow admin user from Secret — never default admin/admin
kubectl -n bigdata rollout status deploy/airflow-api-server --timeout=180s

AF_PASS=$(kubectl -n bigdata get secret airflow-admin-credentials \
  -o jsonpath='{.data.password}' | base64 -d)

kubectl -n bigdata exec deploy/airflow-api-server -- \
  airflow users create \
  --username admin \
  --firstname Fraud \
  --lastname Admin \
  --role Admin \
  --email admin@fraudlab.internal \
  --password "${AF_PASS}"

unset AF_PASS
echo "✅ Airflow admin user created from Secret"

kubectl -n bigdata exec airflow-postgresql-0 -- \
  bash -c 'PGPASSWORD="${POSTGRES_PASSWORD}" psql -U postgres \
    -c "CREATE DATABASE iceberg_catalog;" 2>/dev/null || true'
echo "✅ Iceberg catalog database created in PostgreSQL"
```

### Commit Phase 6.1

```bash
cd $REPO_ROOT
git add gitops/bigdata/airflow/airflow-values.yaml
git commit -m "Phase 6: Airflow values"
git push origin main
```

---


### 6.15 Run init-iceberg-schema (one-time DDL — no DAG)

```bash
export GIT_SHA=$(cat $REPO_ROOT/k8s-bigdata/spark-fraud-job/_rendered/LAST_BUILT_SHA)

mkdir -p "$BD_ROOT/spark-fraud-job/_rendered"

out="$BD_ROOT/spark-fraud-job/_rendered/init-iceberg-schema.${GIT_SHA}.yaml"
envsubst < "$BD_ROOT/spark-fraud-job/init-iceberg-schema.yaml.tpl" > "$out"

grep -nE '^\s*image:\s*' "$out"

kubectl -n bigdata delete sparkapplication init-iceberg-schema \
  --ignore-not-found=true
kubectl -n bigdata wait \
  --for=delete sparkapplication/init-iceberg-schema \
  --timeout=60s 2>/dev/null || true

kubectl -n bigdata apply -f "$out"

kubectl -n bigdata wait sparkapplication/init-iceberg-schema \
  --for=jsonpath='{.status.applicationState.state}'=COMPLETED \
  --timeout=180s

kubectl -n bigdata logs init-iceberg-schema-driver --tail=20
```

---


### 6.2 Deploy SparkApplication (imperative — NOT ArgoCD — requires airflow-postgresql secret from 6.1)

```bash
# SHA match check — ensures the latest local git commit  matches the last built image on local filesystem.
# HEAD_SHA  = Reads the latest commit SHA from the local git repo on the server
# BUILT_SHA = the SHA used when the image was last built (read from the rendered filename on disk).
# If they differ, code was committed after the last build — rebuild before deploying.

HEAD_SHA=$(git -C $REPO_ROOT rev-parse --short HEAD)
BUILT_SHA=$(ls -t $BD_ROOT/spark-fraud-job/_rendered/buildkit-fraud-spark.*.yaml \
  | head -1 | sed 's/.*buildkit-fraud-spark\.\(.*\)\.yaml/\1/')
echo "Last built image : ${BUILT_SHA}"
echo "Current HEAD     : ${HEAD_SHA}"
if [ "${BUILT_SHA}" != "${HEAD_SHA}" ]; then
  echo "⚠️  SHA mismatch — re-run '### 5.1d Build + Push Spark Job Image' before continuing"
  exit 1
fi
echo "✅ SHA match — image ghcr.io/${ORG}/fraud-spark-job:${HEAD_SHA} exists, safe to deploy"
```

```bash
set -euo pipefail

export GIT_SHA=$(cat $REPO_ROOT/k8s-bigdata/spark-fraud-job/_rendered/LAST_BUILT_SHA)

mkdir -p "$BD_ROOT/spark-fraud-job/_rendered"

out="$BD_ROOT/spark-fraud-job/_rendered/spark-fraud-stream.${GIT_SHA}.yaml"
envsubst < "$BD_ROOT/spark-fraud-job/spark-fraud-stream.yaml.tpl" > "$out"

# Sanity check image
grep -nE '^\s*image:\s*' "$out"

# Delete existing + wait for full removal
kubectl -n bigdata delete sparkapplication spark-fraud-stream \
  --ignore-not-found=true
kubectl -n bigdata delete pod \
  -l sparkoperator.k8s.io/app-name=spark-fraud-stream \
  --ignore-not-found=true
kubectl -n bigdata wait \
  --for=delete sparkapplication/spark-fraud-stream \
  --timeout=180s 2>/dev/null || true

# Apply
kubectl -n bigdata apply -f "$out"

# Store current Spark image in Airflow Variable for DAGs to reference
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow variables set FRAUD_SPARK_IMAGE \
  "ghcr.io/jjcorderomejia/fraud-spark-job:${GIT_SHA}"
echo "✅ FRAUD_SPARK_IMAGE variable set in Airflow"

# Minimum risk profile row count gate — configurable without code change
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow variables set RISK_PROFILE_MIN_COUNT "45"
echo "✅ RISK_PROFILE_MIN_COUNT variable set in Airflow"

# Monitor
kubectl -n bigdata wait sparkapplication/spark-fraud-stream \
  --for=jsonpath='{.status.applicationState.state}'=RUNNING \
  --timeout=120s
kubectl -n bigdata get sparkapplication spark-fraud-stream

#Validation
# Driver pod running
kubectl -n bigdata get pods | grep spark-fraud-stream

# No errors in driver
kubectl -n bigdata logs spark-fraud-stream-driver --tail=100
kubectl -n bigdata logs spark-fraud-stream-driver --tail=50 | grep -v INFO
```

---

### 6.3 DAGs: Reconcile + Feature Refresh + Compaction

 ```
  Server (Hetzner)
       │
       ├── Step 1:  cat > infra/spark/compact_iceberg_spark_app.yaml
       │
       ├── Step 1b: kubectl create configmap compact-iceberg-spark-app
       │              └── from infra/spark/compact_iceberg_spark_app.yaml
       │
       ├── Step 2:  cat > dags/*.py  (on server, in repo)
       │              └── git commit + git push
       │
       └── Step 3:  helm upgrade (gitSync enabled)
                         │
                         ▼  git-sync sidecar polls every 60s
  ┌──────────────────────────────────────────────────┐
  │  Airflow dag-processor pod                       │
  │                                                  │
  │  [1] git-sync sidecar: pulls dags/ from repo     │
  │  [2] dag-processor: parses .py files             │
  │  [3] Airflow Variable: COMPACT_ICEBERG_SPARK_SPEC │
  │      (bootstrapped from ConfigMap at setup)       │
  │                                                   │
  │  fraud_feature_refresh.py:                        │
  │    Variable.get("COMPACT_ICEBERG_SPARK_SPEC")     │
  │    → patches spec.image = FRAUD_SPARK_IMAGE       │
  └──────────────────────────────────────────────────┘

  ▎ DAG files live in the repo (dags/) — every change flows through git, auto-synced to cluster.

  ---
 
  DAG 1 — fraud_hourly_reconcile

  Every hour
      │
      ▼
  ┌──────────────────────────────┐
  │ check_redpanda_consumer_lag  │  rpk group describe fraud-stream-consumer
  │ (redpanda pod)               │  → exit 1 if lag > 10000
  └──────────────┬───────────────┘
                 │ success only
                 ▼
  ┌──────────────────────────────┐
  │ check_starrocks_count        │  SELECT count(*) WHERE ingest_time >= NOW()-1h
  │ (starrocks pod)              │  → exit 1 if count = 0  (pipeline stalled)
  └──────────────────────────────┘

  Purpose: catch silent failures — if transactions stopped flowing, this alerts within the hour.

  ---
  DAG 2 — fraud_daily_feature_refresh

  Every day at 02:00
      │
      ▼
  ┌──────────────────────────────────────────────────────┐
  │ refresh_risk_profiles                                │
  │ (starrocks pod)                                      │
  │                                                      │
  │  INSERT OVERWRITE fraud.risk_profiles                │
  │  SELECT user_id,                                     │
  │         AVG(amount), STDDEV(amount),                 │  ← 30-day rolling window
  │         COUNT(*)/24.0,                               │
  │         MAX(merchant_lat), MAX(merchant_lon), NOW()  │
  │  FROM fraud.transactions                             │
  │  WHERE event_time >= NOW() - 30 days                 │
  └──────────────────────┬───────────────────────────────┘
                         │ success only
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │ verify_risk_profiles                                 │
  │ (starrocks pod)                                      │
  │                                                      │
  │  SELECT count(*) FROM fraud.risk_profiles            │
  │  → exit 1 if count = 0                               │
  └──────────────────────┬───────────────────────────────┘
                         │ success only
                         ▼
  ┌──────────────────────────────────────────────────────┐
  │ compact_iceberg_tables                               │
  │ (Spark Operator — cluster mode, driver + executors)  │
  │                                                      │
  │  SparkApplication CR submitted via Spark Operator    │
  │  Manifest loaded from ConfigMap mount                │
  │  (/opt/spark-manifests/), image patched at           │
  │  runtime from FRAUD_SPARK_IMAGE Airflow Variable     │
  │  → rewrites many small files → fewer large files     │  ← MinIO / Iceberg housekeeping
  └──────────────────────────────────────────────────────┘

  ---
  Secrets wiring (nothing hardcoded)

  K8s Secret                  Key                    Used by
  ─────────────────────────   ────────────────────   ──────────────────────────────
  starrocks-credentials       root-password          both DAGs (StarRocks queries)
  minio-secret                MINIO_ROOT_USER        compact_iceberg (Iceberg S3)
                              MINIO_ROOT_PASSWORD
  airflow-postgresql          postgres-password      compact_iceberg (Iceberg JDBC catalog)
```

**Step 1 — Create `compact_iceberg_spark_app.yaml`**

The SparkApplication manifest lives in `infra/spark/` — version controlled, single source of truth. A K8s ConfigMap is created from this file and the spec is bootstrapped into the `COMPACT_ICEBERG_SPARK_SPEC` Airflow Variable (Step 5c.5). The DAG reads from that Variable at parse time and patches the `image` field at runtime from `FRAUD_SPARK_IMAGE` — no filesystem dependency at parse or execution time.

```bash
mkdir -p $REPO_ROOT/infra/spark

cat > $REPO_ROOT/infra/spark/compact_iceberg_spark_app.yaml <<'YAMLEOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: compact-iceberg
  namespace: bigdata
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  # image injected at runtime by fraud_feature_refresh DAG via FRAUD_SPARK_IMAGE Airflow Variable
  image: "__SPARK_IMAGE__"
  imagePullPolicy: Always
  mainApplicationFile: "local:///opt/spark/jobs/compact_iceberg.py"
  sparkVersion: "3.5.0"
  restartPolicy:
    type: Never
  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark
    env:
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
  executor:
    cores: 2
    instances: 2
    memory: "2g"
    env:
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
YAMLEOF
```

**Step 1b — Create `risk_profiles_to_iceberg_spark_app.yaml`**

```bash
cat > $REPO_ROOT/infra/spark/risk_profiles_to_iceberg_spark_app.yaml <<'YAMLEOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: risk-profiles-to-iceberg
  namespace: bigdata
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  # image injected at runtime by fraud_feature_refresh DAG via FRAUD_SPARK_IMAGE Airflow Variable
  image: "__SPARK_IMAGE__"
  imagePullPolicy: Always
  mainApplicationFile: "local:///opt/spark/jobs/risk_profiles_to_iceberg.py"
  sparkVersion: "3.5.0"
  restartPolicy:
    type: Never
  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark
    env:
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
      - name: STARROCKS_PASSWORD
        valueFrom:
          secretKeyRef:
            name: starrocks-credentials
            key: root-password
  executor:
    cores: 2
    instances: 2
    memory: "2g"
    env:
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
      - name: STARROCKS_PASSWORD
        valueFrom:
          secretKeyRef:
            name: starrocks-credentials
            key: root-password
YAMLEOF
```

**Step 2 — Write DAGs to repo**

DAG files live in `dags/` in the repo. git-sync syncs them to the cluster automatically — no pod access required. DAGs written directly into a pod or PVC are lost on restart; git-sync re-clones from the repo on every pod start, so DAGs survive restarts and every change flows through git.

> **Note:** `fraud_reconcile.py` (and all other DAGs) are referenced implicitly — there is no explicit `kubectl apply` or manifest for them. The connection is `airflow-values.yaml` → `dags.gitSync.subPath: dags`, which mounts the entire `dags/` directory. Airflow's DAG processor auto-discovers every `.py` file containing a DAG object.

> **Scope:** git-sync only syncs `dags/*.py`. Spark manifests in `infra/spark/` are deployed via `kubectl apply` (ConfigMap) — they are NOT picked up by git-sync.

```bash
mkdir -p $REPO_ROOT/dags
```

**Step 3 — Write `config.py`**

```bash
cat > $REPO_ROOT/dags/config.py <<'DAGEOF'
"""
Shared pipeline configuration — imported by all fraud DAGs.
PIPELINE_EPOCH: production go-live date, defines the scheduling epoch.
All DAGs use catchup=False — this date anchors audit trails and incident reconstruction.
"""
import pendulum

PIPELINE_EPOCH = pendulum.datetime(2026, 3, 24, tz="UTC")
DAGEOF
```

**How `from config import PIPELINE_EPOCH` resolves:**

Airflow adds `/dags` to `sys.path` at scheduler startup:

```
sys.path = ["/dags", "/usr/lib/python3", ...]
```

When a DAG runs `from config import PIPELINE_EPOCH`, Python searches `sys.path` in order:

```
Is there a config.py in /dags ?  →  YES  →  found  →  import done
```

No packaging, no install — co-location in `/dags` is enough.

**Step 4 — Write `fraud_reconcile.py`**

```bash
cat > $REPO_ROOT/dags/fraud_reconcile.py <<'DAGEOF'
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
            '--brokers fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092 '
            '-X tls.enabled=true -X tls.insecure_skip_verify=true '
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
DAGEOF
```

**Step 5 — Write `fraud_feature_refresh.py`**

```bash
cat > $REPO_ROOT/dags/fraud_feature_refresh.py <<'DAGEOF'
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
_risk_sync_spec = json.loads(Variable.get("RISK_SYNC_ICEBERG_SPARK_SPEC"))
_risk_sync_spec["spec"]["image"] = Variable.get("FRAUD_SPARK_IMAGE")
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

    sync_risk_profiles_iceberg = SparkKubernetesOperator(
        task_id="sync_risk_profiles_iceberg",
        namespace="bigdata",
        application_file=json.dumps(_risk_sync_spec),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        sla=timedelta(minutes=30),
    )

    compact_iceberg = SparkKubernetesOperator(
        task_id="compact_iceberg_tables",
        namespace="bigdata",
        application_file=json.dumps(_compact_spec),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        sla=timedelta(hours=2),
    )

    refresh_risk_profiles >> verify_risk_profiles >> sync_risk_profiles_iceberg >> compact_iceberg
DAGEOF

echo "DAGs written"
ls -la $REPO_ROOT/dags/
```

**Step 5b — Create `customer_csv_to_iceberg_spark_app.yaml`**

Loads customer enrichment data from the `customer-csv` ConfigMap (mounted at `/opt/enrichment/customer.csv`) into `fraud.customers` Iceberg table daily at 02:00. Image patched at runtime from `FRAUD_SPARK_IMAGE` — same pattern as compact_iceberg.

```bash
cat > $REPO_ROOT/infra/spark/customer_csv_to_iceberg_spark_app.yaml <<'YAMLEOF'
apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: customer-csv-to-iceberg
  namespace: bigdata
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  # image injected at runtime by fraud_customer_refresh DAG via FRAUD_SPARK_IMAGE Airflow Variable
  image: "__SPARK_IMAGE__"
  imagePullPolicy: Always
  mainApplicationFile: "local:///opt/spark/jobs/customer_csv_to_iceberg.py"
  sparkVersion: "3.5.0"
  sparkConf:
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.iceberg.type": "jdbc"
    "spark.sql.catalog.iceberg.uri": "jdbc:postgresql://airflow-postgresql.bigdata.svc.cluster.local:5432/iceberg_catalog"
    "spark.sql.catalog.iceberg.jdbc.user": "postgres"
    "spark.sql.catalog.iceberg.warehouse": "s3a://iceberg/warehouse"
    "spark.hadoop.fs.s3a.endpoint": "http://minio.bigdata.svc.cluster.local:9000"
    "spark.hadoop.fs.s3a.path.style.access": "true"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 5
    onSubmissionFailureRetryInterval: 20
  driver:
    cores: 1
    memory: "1g"
    serviceAccount: spark
    env:
      - name: CUSTOMER_CSV_PATH
        value: "/opt/enrichment/customer.csv"
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
    volumeMounts:
      - name: enrichment
        mountPath: /opt/enrichment
  executor:
    instances: 1
    cores: 1
    memory: "1g"
    env:
      - name: MINIO_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_USER
      - name: MINIO_SECRET_KEY
        valueFrom:
          secretKeyRef:
            name: minio-secret
            key: MINIO_ROOT_PASSWORD
      - name: ICEBERG_DB_PASSWORD
        valueFrom:
          secretKeyRef:
            name: airflow-postgresql
            key: postgres-password
    volumeMounts:
      - name: enrichment
        mountPath: /opt/enrichment
  volumes:
    - name: enrichment
      configMap:
        name: customer-csv
YAMLEOF
```

**Step 5c — Recreate ConfigMap with both manifests**

Both Spark manifests live in the same ConfigMap so the single `/opt/spark-manifests/` mount covers all DAGs.

```bash
kubectl -n bigdata create configmap compact-iceberg-spark-app \
  --from-file=compact_iceberg_spark_app.yaml=$REPO_ROOT/infra/spark/compact_iceberg_spark_app.yaml \
  --from-file=customer_csv_to_iceberg_spark_app.yaml=$REPO_ROOT/infra/spark/customer_csv_to_iceberg_spark_app.yaml \
  --from-file=risk_profiles_to_iceberg_spark_app.yaml=$REPO_ROOT/infra/spark/risk_profiles_to_iceberg_spark_app.yaml \
  --dry-run=client -o yaml | kubectl apply -f -
```

```bash
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  ls /opt/spark-manifests/
```

**Step 5d — Bootstrap Spark spec Variables**

Converts each YAML manifest to JSON and stores in Airflow Variables. DAGs read from these Variables at parse and execution time — no filesystem dependency on either the dag-processor or task pods.

```bash
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  python3 -c "
import yaml, json, subprocess
with open('/opt/spark-manifests/compact_iceberg_spark_app.yaml') as f:
    spec = yaml.safe_load(f)
subprocess.run(['airflow', 'variables', 'set', 'COMPACT_ICEBERG_SPARK_SPEC', json.dumps(spec)], check=True)
print('COMPACT_ICEBERG_SPARK_SPEC set')
" 2>/dev/null
```

```bash
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  python3 -c "
import yaml, json, subprocess
with open('/opt/spark-manifests/customer_csv_to_iceberg_spark_app.yaml') as f:
    spec = yaml.safe_load(f)
subprocess.run(['airflow', 'variables', 'set', 'CUSTOMER_CSV_SPARK_SPEC', json.dumps(spec)], check=True)
print('CUSTOMER_CSV_SPARK_SPEC set')
" 2>/dev/null
```

```bash
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  python3 -c "
import yaml, json, subprocess
with open('/opt/spark-manifests/risk_profiles_to_iceberg_spark_app.yaml') as f:
    spec = yaml.safe_load(f)
subprocess.run(['airflow', 'variables', 'set', 'RISK_SYNC_ICEBERG_SPARK_SPEC', json.dumps(spec)], check=True)
print('RISK_SYNC_ICEBERG_SPARK_SPEC set')
" 2>/dev/null
```

**Step 5e — Write `fraud_customer_refresh.py`**

```bash
cat > $REPO_ROOT/dags/fraud_customer_refresh.py <<'DAGEOF'
"""
DAG 3: Daily customer CSV to Iceberg refresh at 02:00.
Loads customer enrichment data from customer-csv ConfigMap into fraud.customers Iceberg table.
SparkKubernetesOperator exit status is the verification — Spark fails fast on write errors.
"""
import json
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

# Load Spark spec from Airflow Variable — no filesystem dependency at parse or execution time
_customer_spec = json.loads(Variable.get("CUSTOMER_CSV_SPARK_SPEC"))
_customer_spec["spec"]["image"] = Variable.get("FRAUD_SPARK_IMAGE")

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
        application_file=json.dumps(_customer_spec),
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=False,
        sla=timedelta(hours=1),
    )
DAGEOF
```

**Step 6 — Commit DAGs + helm upgrade**

> Fresh installation only — DAGs committed here if not already staged incrementally.

```bash
cd $REPO_ROOT
git add dags/config.py
git add dags/fraud_reconcile.py
git add dags/fraud_feature_refresh.py
git add dags/fraud_customer_refresh.py
git commit -m "Phase 6: add fraud DAGs"
git push origin main
```

```bash
# Fresh installation only — enables git-sync for the first time.
# For subsequent DAG additions, git-sync is already running and picks up changes automatically.
source ~/.lab_Huanca && helm upgrade --install airflow apache-airflow/airflow \
  --version 1.19.0 \
  --namespace bigdata \
  -f $REPO_ROOT/gitops/bigdata/airflow/airflow-values.yaml
```

```bash
# Verify git-sync is syncing
sleep 30
kubectl -n bigdata logs deploy/airflow-dag-processor -c git-sync | tail -5
```

> git-sync pulls the repo within 60s, but the dag-processor parses files on its own schedule — a new DAG may not appear immediately after git-sync syncs. If a DAG is not visible after 60s, force an immediate re-parse:

```bash
kubectl -n bigdata exec deploy/airflow-dag-processor -- \
  airflow dags reserialize 2>/dev/null
```

### 6.4 Validate Phase 6

```bash
# Airflow api-server running
kubectl -n bigdata get pods | grep airflow-api-server

AF_SVC_IP=$(kubectl -n bigdata get svc airflow-api-server \
  -o jsonpath='{.spec.clusterIP}')
curl -sS "http://${AF_SVC_IP}:8080/api/v2/monitor/health" | python3 -m json.tool
echo "✅ Airflow healthy at ClusterIP ${AF_SVC_IP}:8080"

# DAGs loaded
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags list 2>/dev/null | grep fraud

# No DAG parse errors
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags list-import-errors 2>/dev/null

# ConfigMap mounted and readable in scheduler
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  cat /opt/spark-manifests/compact_iceberg_spark_app.yaml
```

### 6.45 DAG Bootstrap — unpause + seed initial data

> All DAGs deploy paused. Bootstrap order is load-order dependent:
> 1. `fraud_daily_customer_refresh` — must run first: populates iceberg.fraud.customers
>    which the streaming job reads for enrichment. Blocked on iceberg tables existing (6.15).
> 2. `fraud_daily_feature_refresh` — trigger manually on first bootstrap: populates
>    iceberg.fraud.risk_profiles via sync_risk_profiles_iceberg task. After first seed,
>    runs on schedule (0 2 * * *) automatically.
> 3. `fraud_hourly_reconcile` — unpause only: runs on @hourly schedule.
>    Will self-heal once transactions flow.

```bash
# ── UNPAUSE ALL ───────────────────────────────────────────────────────
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags unpause fraud_daily_customer_refresh 2>/dev/null
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags unpause fraud_daily_feature_refresh 2>/dev/null
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags unpause fraud_hourly_reconcile 2>/dev/null

# ── TRIGGER customer_refresh + WAIT ──────────────────────────────────
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags trigger fraud_daily_customer_refresh 2>/dev/null
RUN_ID=$(kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags list-runs fraud_daily_customer_refresh -o plain 2>/dev/null \
  | awk 'NR>2 && NF {print $2}' | head -1)
echo "Triggered run: ${RUN_ID}"

echo "Waiting for fraud_daily_customer_refresh to complete (max 5 min)..."
for i in $(seq 1 30); do
  STATE=$(kubectl -n bigdata exec deploy/airflow-scheduler -- \
    airflow dags list-runs fraud_daily_customer_refresh \
    -o plain 2>/dev/null | awk -v run="${RUN_ID}" '$2==run {print $3}')
  echo "  attempt ${i}/30 — state: ${STATE}"
  if [ "${STATE}" = "success" ]; then
    echo "✅ fraud_daily_customer_refresh succeeded — iceberg.fraud.customers populated"
    break
  fi
  if [ "${STATE}" = "failed" ]; then
    echo "❌ fraud_daily_customer_refresh FAILED — check driver logs"; exit 1
  fi
  sleep 10
done

# ── CONFIRM ───────────────────────────────────────────────────────────
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags list-runs fraud_daily_customer_refresh 2>/dev/null

# ── TRIGGER feature_refresh + WAIT (seeds iceberg.fraud.risk_profiles) ──
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags trigger fraud_daily_feature_refresh 2>/dev/null
FR_RUN_ID=$(kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags list-runs fraud_daily_feature_refresh -o plain 2>/dev/null \
  | awk 'NR>2 && NF {print $2}' | head -1)
echo "Triggered run: ${FR_RUN_ID}"

echo "Waiting for sync_risk_profiles_iceberg to complete (max 10 min)..."
for i in $(seq 1 60); do
  TASK_STATE=$(kubectl -n bigdata exec deploy/airflow-scheduler -- \
    airflow tasks state fraud_daily_feature_refresh sync_risk_profiles_iceberg \
    "${FR_RUN_ID}" 2>/dev/null || echo "pending")
  echo "  attempt ${i}/60 — sync_risk_profiles_iceberg: ${TASK_STATE}"
  if [ "${TASK_STATE}" = "success" ]; then
    echo "✅ sync_risk_profiles_iceberg succeeded — iceberg.fraud.risk_profiles seeded"
    break
  fi
  if [ "${TASK_STATE}" = "failed" ]; then
    echo "❌ sync_risk_profiles_iceberg FAILED — check driver logs"; exit 1
  fi
  sleep 10
done
```

---


### 6.46 Validate DAG Bootstrap

```bash
# ── ALL THREE DAGs UNPAUSED ───────────────────────────────────────────
kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags list 2>/dev/null | grep fraud
# Expect: is_paused = False for all three

# ── customer_refresh HAS A SUCCESSFUL RUN ────────────────────────────
RUN_COUNT=$(kubectl -n bigdata exec deploy/airflow-scheduler -- \
  airflow dags list-runs fraud_daily_customer_refresh \
  --state success -o plain 2>/dev/null | grep -c "success" || true)
if [ "${RUN_COUNT}" -ge 1 ]; then
  echo "✅ fraud_daily_customer_refresh: ${RUN_COUNT} successful run(s)"
else
  echo "❌ fraud_daily_customer_refresh has no successful runs"; exit 1
fi

# ── iceberg.fraud.customers IS NOT EMPTY — check driver logs ─────────
DRIVER_POD=$(kubectl -n bigdata get pods \
  --sort-by=.metadata.creationTimestamp \
  -o jsonpath='{.items[*].metadata.name}' \
  | tr ' ' '\n' | grep "customer-csv-to-iceberg" | grep "driver" | tail -1)
kubectl -n bigdata logs "${DRIVER_POD}" --tail=5 2>/dev/null
# Expect: ✅ N customer records written to iceberg.fraud.customers
```

---


### 6.5 Commit Phase 6 to GitHub

> Fresh installation only — DAG files are committed incrementally during development (Steps 5d, 6). Only the Spark manifests need committing here if not already staged.

```bash
cd $REPO_ROOT
git add infra/spark/compact_iceberg_spark_app.yaml
git add infra/spark/customer_csv_to_iceberg_spark_app.yaml
git add infra/spark/risk_profiles_to_iceberg_spark_app.yaml
git commit -m "Phase 6: Airflow + reconcile DAG + feature refresh DAG + customer refresh DAG + Iceberg compaction"
git push origin main
```

---

## ════════════════════════════════════════
## PHASE 7 — FASTAPI BACKEND
## ════════════════════════════════════════

> Same pattern as Cassandra lab backend-api.
> Extended with fraud-specific endpoints.
> Built with BuildKit, pushed to GHCR with GIT_SHA.
>
> Production fixes applied vs original design:
> - confluent-kafka replaces kafka-python (unmaintained since 2022, Redpanda compat issues)
> - produce() + flush(timeout=5) + delivery callback — no infinite block, error propagated to caller
> - MySQLConnectionPool (pool_size=5) replaces per-request new connection — try/finally on all cursors
> - Query(le=1000) cap on /api/fraud-scores and /api/top-risky-users — prevents unbounded queries
> - API key auth via X-Api-Key header — loaded from K8s Secret api-key-secret
>   /api/health exempt from auth — required for K8s httpGet readiness/liveness probes
>   Key is visible in browser DevTools by design — machine-to-machine access control layer
>   User-facing login (JWT) deferred to Phase 8+
> - runAsNonRoot: true, runAsUser: 1000 on Deployment — matches Spark pod security model
> - Liveness probe added (failureThreshold: 3, timeoutSeconds: 5)
> - Readiness probe checks Redpanda + StarRocks connectivity before passing
> - git add per-file in commit step

### 7.1 K8s API Key Secret

Generate a random 32-byte hex key and persist in K8s before deploying the app.

```bash
kubectl -n apps create secret generic api-key-secret \
  --from-literal=api-key="$(openssl rand -hex 32)" \
  --dry-run=client -o yaml | kubectl apply -f -
```

```bash
# Confirm the secret was created without exposing the value
kubectl -n apps get secret api-key-secret -o jsonpath='{.metadata.creationTimestamp}'
```

### 7.2 FastAPI App Code

```bash
mkdir -p $REPO_ROOT/k8s-apps/backend-api

cat > $REPO_ROOT/k8s-apps/backend-api/main.py <<'PYEOF'
"""
Fraud Detection Backend API
- POST /api/transaction      → produce to Redpanda
- GET  /api/fraud-scores     → query StarRocks
- GET  /api/health           → readiness + liveness probe (no auth)
- GET  /api/stats            → dashboard stats
- GET  /api/top-risky-users  → top risky users (24h)

Auth: X-Api-Key header required on all routes except /api/health.
Key loaded from API_KEY env var (K8s Secret: api-key-secret).
The key is visible in browser DevTools by design — this is a machine-to-machine
access control layer that blocks unauthenticated callers. User-facing login (JWT)
is deferred to Phase 8+.
"""
import os
import uuid
import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Depends, APIRouter, Query
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, validator
from mysql.connector import pooling
from confluent_kafka import Producer, KafkaException

# ── Config ─────────────────────────────────────────────────────────
REDPANDA_BOOTSTRAP = os.environ["REDPANDA_BOOTSTRAP"]
STARROCKS_HOST     = os.environ.get("STARROCKS_HOST", "starrocks-fe.bigdata.svc.cluster.local")
STARROCKS_PORT     = int(os.environ.get("STARROCKS_PORT", "9030"))
STARROCKS_DB       = os.environ.get("STARROCKS_DB", "fraud")
STARROCKS_PASSWORD = os.environ["STARROCKS_PASSWORD"]
TOPIC_RAW          = os.environ.get("TOPIC_RAW", "transactions-raw")
API_KEY            = os.environ["API_KEY"]

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":%(message)s}',
)
log = logging.getLogger("fraud-api")

# ── Kafka Producer (confluent-kafka) ────────────────────────────────
producer = Producer({
    "bootstrap.servers":               REDPANDA_BOOTSTRAP,
    "acks":                            "all",
    "retries":                         3,
    "retry.backoff.ms":                200,
    "security.protocol":               "SSL",
    "ssl.ca.location":                 "/etc/redpanda-certs/ca.crt",
    "ssl.endpoint.identification.algorithm": "none",
})

# ── StarRocks Connection Pool ───────────────────────────────────────
db_pool = pooling.MySQLConnectionPool(
    pool_name="sr_pool",
    pool_size=5,
    host=STARROCKS_HOST,
    port=STARROCKS_PORT,
    user="root",
    password=STARROCKS_PASSWORD,
    database=STARROCKS_DB,
)

def get_sr_conn():
    return db_pool.get_connection()

# ── Auth ────────────────────────────────────────────────────────────
api_key_header = APIKeyHeader(name="X-Api-Key", auto_error=False)

def verify_api_key(key: str = Depends(api_key_header)):
    if key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid or missing API key")
    return key

# ── App + Protected Router ──────────────────────────────────────────
app    = FastAPI(title="Fraud Detection API", version="1.0.0")
router = APIRouter(dependencies=[Depends(verify_api_key)])

# ── Models ──────────────────────────────────────────────────────────
class Transaction(BaseModel):
    user_id:      str
    amount:       float
    merchant_id:  str
    merchant_lat: Optional[float] = None
    merchant_lon: Optional[float] = None
    status:       str = "pending"

    @validator("amount")
    def amount_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError("amount must be positive")
        return v

# ── Health — no auth, required for K8s probes ───────────────────────
@app.get("/api/health")
def health():
    redpanda_ok  = False
    starrocks_ok = False

    try:
        meta = producer.list_topics(timeout=2)
        redpanda_ok = meta is not None
    except KafkaException as e:
        log.error('"event":"health_redpanda_fail","error":"%s"', e)

    try:
        conn = get_sr_conn()
        conn.close()
        starrocks_ok = True
    except Exception as e:
        log.error('"event":"health_starrocks_fail","error":"%s"', e)

    if not redpanda_ok or not starrocks_ok:
        raise HTTPException(
            status_code=503,
            detail={"redpanda": redpanda_ok, "starrocks": starrocks_ok},
        )

    return {"status": "ok", "service": "fraud-api", "time": datetime.utcnow().isoformat()}

# ── Protected Routes ─────────────────────────────────────────────────
@router.post("/api/transaction", status_code=202)
def create_transaction(txn: Transaction):
    payload = {
        "transaction_id": str(uuid.uuid4()),   # UUID4 — never $RANDOM
        "user_id":        txn.user_id,
        "amount":         txn.amount,
        "merchant_id":    txn.merchant_id,
        "merchant_lat":   txn.merchant_lat,
        "merchant_lon":   txn.merchant_lon,
        "status":         txn.status,
        "timestamp":      datetime.utcnow().isoformat() + "Z",
    }

    delivery_error = {}

    def on_delivery(err, msg):
        if err:
            delivery_error["err"] = err

    producer.produce(TOPIC_RAW, value=json.dumps(payload).encode("utf-8"), callback=on_delivery)
    remaining = producer.flush(timeout=5)

    if remaining > 0 or delivery_error.get("err"):
        log.error(
            '"event":"produce_fail","transaction_id":"%s","error":"%s"',
            payload["transaction_id"], delivery_error.get("err", "timeout"),
        )
        raise HTTPException(status_code=503, detail="Message delivery failed — retry")

    log.info(
        '"event":"transaction_queued","transaction_id":"%s","user_id":"%s"',
        payload["transaction_id"], txn.user_id,
    )
    return {"transaction_id": payload["transaction_id"], "status": "queued"}

@router.get("/api/fraud-scores")
def get_fraud_scores(limit: int = Query(default=20, ge=1, le=1000)):
    conn = get_sr_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT transaction_id, user_id, fraud_score, reasons, flagged_at
            FROM fraud_scores
            ORDER BY flagged_at DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return {"fraud_scores": rows}
    finally:
        conn.close()

@router.get("/api/stats")
def get_stats():
    conn = get_sr_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT
              count(*)                                        AS total_transactions,
              sum(case when is_flagged then 1 else 0 end)    AS flagged_count,
              avg(fraud_score)                               AS avg_fraud_score,
              max(ingest_time)                               AS last_ingest
            FROM fraud.transactions
            WHERE event_time >= date_sub(now(), interval 1 hour)
        """)
        stats = cur.fetchone()
        cur.close()
        return stats
    finally:
        conn.close()

@router.get("/api/top-risky-users")
def top_risky_users(limit: int = Query(default=10, ge=1, le=1000)):
    conn = get_sr_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("""
            SELECT user_id, count(*) AS flagged_count, max(fraud_score) AS max_score
            FROM fraud_scores
            WHERE flagged_at >= date_sub(now(), interval 24 hour)
            GROUP BY user_id
            ORDER BY flagged_count DESC
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        cur.close()
        return {"top_risky_users": rows}
    finally:
        conn.close()

app.include_router(router)
PYEOF
```

```bash
cat > $REPO_ROOT/k8s-apps/backend-api/requirements.txt <<'EOF'
fastapi==0.111.0
uvicorn==0.30.1
pydantic==2.7.1
confluent-kafka==2.4.0
mysql-connector-python==8.4.0
EOF
```

### 7.3 Dockerfile + BuildKit Template

```bash
cat > $REPO_ROOT/k8s-apps/backend-api/Dockerfile <<'EOF'
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

RUN adduser --disabled-password --gecos '' --uid 1000 appuser
USER appuser

EXPOSE 8000
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
EOF
```

```bash
cat > $REPO_ROOT/k8s-apps/backend-api/buildkit-job.yaml.tpl <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: buildkit-fraud-api-${GIT_SHA}
  namespace: bigdata
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: buildkit
      imagePullSecrets:
        - name: ghcr-creds
      initContainers:
        - name: copy-context
          image: busybox:1.36
          command: ['cp', '-r', '/src/.', '/workspace/']
          volumeMounts:
            - name: src
              mountPath: /src
            - name: workspace
              mountPath: /workspace
      containers:
        - name: buildctl
          image: moby/buildkit:v0.13.2
          command:
            - buildctl
            - --addr
            - tcp://buildkitd.bigdata.svc.cluster.local:1234
            - build
            - --frontend=dockerfile.v0
            - --local
            - context=/workspace
            - --local
            - dockerfile=/workspace
            - --output
            - type=image,name=ghcr.io/${ORG}/fraud-api:${GIT_SHA},push=true
          volumeMounts:
            - name: workspace
              mountPath: /workspace
            - name: ghcr-secret
              mountPath: /kaniko/.docker
              readOnly: true
          env:
            - name: DOCKER_CONFIG
              value: /kaniko/.docker
      volumes:
        - name: workspace
          emptyDir: {}
        - name: src
          hostPath:
            path: ${HOST_BACKEND_ROOT}
            type: Directory
        - name: ghcr-secret
          secret:
            secretName: ghcr-creds
            items:
              - key: .dockerconfigjson
                path: config.json
EOF
```

```bash
cat > $REPO_ROOT/k8s-apps/backend-api/backend-api.yaml.tpl <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: backend-api
  namespace: apps
spec:
  replicas: 1
  selector:
    matchLabels:
      app: backend-api
  template:
    metadata:
      labels:
        app: backend-api
    spec:
      imagePullSecrets:
        - name: ghcr-creds
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
        - name: backend-api
          image: ghcr.io/${ORG}/fraud-api:${GIT_SHA}
          imagePullPolicy: Always
          ports:
            - containerPort: 8000
          env:
            - name: REDPANDA_BOOTSTRAP
              value: "fraud-redpanda-0.fraud-redpanda.bigdata.svc.cluster.local:9092"
            - name: STARROCKS_HOST
              value: "starrocks-fe.bigdata.svc.cluster.local"
            - name: STARROCKS_PORT
              value: "9030"
            - name: STARROCKS_DB
              value: "fraud"
            - name: STARROCKS_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: starrocks-credentials
                  key: root-password
            - name: TOPIC_RAW
              value: "transactions-raw"
            - name: API_KEY
              valueFrom:
                secretKeyRef:
                  name: api-key-secret
                  key: api-key
          resources:
            requests:
              cpu: "100m"
              memory: "256Mi"
            limits:
              cpu: "500m"
              memory: "512Mi"
          readinessProbe:
            httpGet:
              path: /api/health
              port: 8000
            initialDelaySeconds: 10
            periodSeconds: 5
            timeoutSeconds: 5
          livenessProbe:
            httpGet:
              path: /api/health
              port: 8000
            initialDelaySeconds: 30
            periodSeconds: 10
            timeoutSeconds: 5
            failureThreshold: 3
          volumeMounts:
            - name: redpanda-certs
              mountPath: /etc/redpanda-certs
              readOnly: true
      volumes:
        - name: redpanda-certs
          secret:
            secretName: fraud-redpanda-default-root-certificate
            items:
              - key: ca.crt
                path: ca.crt
---
apiVersion: v1
kind: Service
metadata:
  name: backend-api
  namespace: apps
spec:
  selector:
    app: backend-api
  ports:
    - port: 8000
      targetPort: 8000
EOF
```

### 7.4 Build + Deploy Backend API

```bash
source ~/.lab_Huanca
```

```bash
set -euo pipefail
mkdir -p "$HOST_BACKEND_ROOT/_rendered"
```

```bash
out_build="$HOST_BACKEND_ROOT/_rendered/buildkit-fraud-api.${GIT_SHA}.yaml"
envsubst < "$HOST_BACKEND_ROOT/buildkit-job.yaml.tpl" > "$out_build"
```

```bash
kubectl -n bigdata delete job buildkit-fraud-api-${GIT_SHA} --ignore-not-found
```

```bash
kubectl -n bigdata apply -f "$out_build"
```

```bash
kubectl -n bigdata wait --for=condition=complete job/buildkit-fraud-api-${GIT_SHA} --timeout=900s
```

```bash
out_deploy="$HOST_BACKEND_ROOT/_rendered/backend-api.${GIT_SHA}.yaml"
envsubst < "$HOST_BACKEND_ROOT/backend-api.yaml.tpl" > "$out_deploy"
```

```bash
# Copy starrocks-credentials from bigdata to apps namespace
kubectl -n bigdata get secret starrocks-credentials -o yaml \
  | sed 's/namespace: bigdata/namespace: apps/' \
  | kubectl -n apps apply -f -
# Copy Redpanda CA cert from bigdata to apps namespace
kubectl -n bigdata get secret fraud-redpanda-default-root-certificate -o yaml \
  | sed 's/namespace: bigdata/namespace: apps/' \
  | kubectl -n apps apply -f -
```

```bash
kubectl -n apps apply -f "$out_deploy"
```

```bash
kubectl -n apps rollout status deploy/backend-api --timeout=180s
```

```bash
# Validate image SHA
kubectl -n apps get deploy backend-api \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

### 7.5 Commit Phase 7 to GitHub

```bash
cd $REPO_ROOT
git add k8s-apps/backend-api/main.py
git add k8s-apps/backend-api/requirements.txt
git add k8s-apps/backend-api/Dockerfile
git add k8s-apps/backend-api/buildkit-job.yaml.tpl
git add k8s-apps/backend-api/backend-api.yaml.tpl
git commit -m "Phase 7: FastAPI backend with fraud endpoints"
git push origin main
```

---

### 7.6 Validate Phase 7

```bash
# Pod running and image SHA matches HEAD
kubectl -n apps get pods | grep backend-api
kubectl -n apps get deploy backend-api \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

```bash
# Health endpoint — use python3 urllib (curl/wget not in minimal image)
kubectl -n apps exec deploy/backend-api -- \
  python3 -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8000/api/health').read().decode())"
```

---

## ════════════════════════════════════════
## PHASE 8 — REACT FRAUD DASHBOARD + INGRESS
## ════════════════════════════════════════

> React dashboard served by Nginx.
> Same single-IP Hetzner Ingress pattern as Cassandra lab.

### 8.1 React App

```bash
mkdir -p $REPO_ROOT/k8s-apps/fraud-ui/src

cat > $REPO_ROOT/k8s-apps/fraud-ui/src/App.jsx <<'JSXEOF'
import { useState, useEffect, useRef } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine
} from "recharts";

const API = "/api";

const SPINNER = (
  <div style={{
    width: 40, height: 40, margin: "80px auto",
    border: "4px solid #e8e8e8",
    borderTop: "4px solid #e94560",
    borderRadius: "50%",
    animation: "spin 0.8s linear infinite"
  }} />
);

function StatCard({ label, value, color }) {
  return (
    <div style={{
      background: "#fff", border: "1px solid #e8e8e8",
      borderTop: `3px solid ${color}`,
      borderRadius: 8, padding: "16px 24px", minWidth: 160, flex: "1 1 160px",
      boxShadow: "0 1px 4px rgba(0,0,0,0.06)"
    }}>
      <div style={{ color: "#555", fontSize: 12, textTransform: "uppercase", letterSpacing: 1 }}>{label}</div>
      <div style={{ color: "#1a1a2e", fontSize: 32, fontWeight: "bold", marginTop: 4 }}>{value ?? "—"}</div>
    </div>
  );
}

function ScoreBadge({ score }) {
  const color = score >= 80 ? "#e53e3e" : score >= 60 ? "#dd6b20" : "#d69e2e";
  const bg    = score >= 80 ? "#fff5f5" : score >= 60 ? "#fffaf0" : "#fffff0";
  return (
    <span style={{
      background: bg, color, border: `1px solid ${color}`,
      borderRadius: 4, padding: "2px 8px", fontWeight: "bold", fontSize: 12
    }}>
      {score}
    </span>
  );
}

function FraudTable({ scores }) {
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 13 }}>
        <thead>
          <tr style={{ background: "#f7f8fa", color: "#555", fontSize: 12, textTransform: "uppercase", letterSpacing: 1 }}>
            {["Transaction ID", "User", "Score", "Reasons", "Flagged At"].map(h => (
              <th key={h} style={{ padding: "10px 14px", textAlign: "left", fontWeight: 600, borderBottom: "1px solid #e8e8e8" }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {scores.map((s, i) => (
            <tr key={s.transaction_id}
              style={{ background: i % 2 ? "#fafafa" : "#fff", borderBottom: "1px solid #f0f0f0" }}>
              <td style={{ padding: "8px 14px", fontFamily: "monospace", fontSize: 11, color: "#777" }}>
                {s.transaction_id?.slice(0, 8)}…
              </td>
              <td style={{ padding: "8px 14px", color: "#1a1a2e", fontWeight: 500 }}>{s.user_id}</td>
              <td style={{ padding: "8px 14px" }}><ScoreBadge score={s.fraud_score} /></td>
              <td style={{ padding: "8px 14px", color: "#666", fontSize: 12 }}>{s.reasons}</td>
              <td style={{ padding: "8px 14px", color: "#777", fontSize: 12 }}>
                {s.flagged_at?.slice(0, 19).replace("T", " ")}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <div style={{ color: "#777", fontSize: 12, padding: "6px 14px" }}>
        Showing {scores.length} most recent alerts
      </div>
    </div>
  );
}

function ScoreChart({ scores }) {
  const data = [...scores].reverse().map(s => ({
    time: s.flagged_at?.slice(11, 19),
    score: s.fraud_score,
    user: s.user_id,
  }));
  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={data} margin={{ top: 8, right: 16, left: 0, bottom: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
        <XAxis dataKey="time" tick={{ fill: "#bbb", fontSize: 10 }} />
        <YAxis domain={[0, 100]} tick={{ fill: "#bbb", fontSize: 10 }} />
        <Tooltip
          contentStyle={{ background: "#fff", border: "1px solid #e8e8e8", borderRadius: 6, boxShadow: "0 2px 8px rgba(0,0,0,0.1)" }}
          labelStyle={{ color: "#666" }}
          itemStyle={{ color: "#e94560" }}
          formatter={(v, _, p) => [`${v} pts`, p.payload.user]}
        />
        <ReferenceLine y={60} stroke="#e53e3e" strokeDasharray="4 2" label={{ value: "Threshold", fill: "#e53e3e", fontSize: 10 }} />
        <Bar dataKey="score" fill="#e94560" radius={[3, 3, 0, 0]} />
      </BarChart>
    </ResponsiveContainer>
  );
}

export default function App() {
  const [stats,      setStats]      = useState(null);
  const [scores,     setScores]     = useState([]);
  const [topUsers,   setTopUsers]   = useState([]);
  const [loading,    setLoading]    = useState(true);
  const [error,      setError]      = useState(null);
  const backoffRef  = useRef(0);
  const errorCount  = useRef(0);

  const fetchData = async () => {
    if (backoffRef.current > 0) { backoffRef.current--; return; }
    try {
      const [s, f, t] = await Promise.all([
        fetch(`${API}/stats`).then(r => r.json()),
        fetch(`${API}/fraud-scores?limit=20`).then(r => r.json()),
        fetch(`${API}/top-risky-users`).then(r => r.json()),
      ]);
      errorCount.current = 0;
      backoffRef.current = 0;
      setError(null);
      setStats(s);
      setScores(f.fraud_scores || []);
      setTopUsers(t.top_risky_users || []);
    } catch (e) {
      errorCount.current++;
      backoffRef.current = Math.min(Math.pow(2, errorCount.current - 1), 32);
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    const id = setInterval(fetchData, 5000);
    return () => clearInterval(id);
  }, []);

  return (
    <>
      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        * { box-sizing: border-box; }
        body { margin: 0; background: #f5f6fa; }
      `}</style>
      <div style={{ minHeight: "100vh", color: "#1a1a2e", fontFamily: "system-ui, sans-serif" }}>

        {/* Header */}
        <div style={{ background: "#fff", borderBottom: "1px solid #e8e8e8", padding: "16px 32px",
                      display: "flex", justifyContent: "space-between", alignItems: "center",
                      boxShadow: "0 1px 4px rgba(0,0,0,0.06)" }}>
          <div>
            <h1 style={{ color: "#e94560", margin: 0, fontSize: 20, fontWeight: 700, letterSpacing: 1 }}>
              FRAUD DETECTION
            </h1>
            <div style={{ color: "#777", fontSize: 12, marginTop: 2 }}>
              Redpanda → Spark → StarRocks
            </div>
          </div>
          <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span style={{ width: 8, height: 8, borderRadius: "50%",
                           background: error ? "#e53e3e" : "#38a169", display: "inline-block" }} />
            <span style={{ color: error ? "#e53e3e" : "#38a169", fontSize: 12, fontWeight: 500 }}>
              {error ? "Disconnected" : "Live"}
            </span>
            <span style={{ color: "#888", fontSize: 12, marginLeft: 8 }}>
              {stats?.last_ingest?.slice(0, 19).replace("T", " ")} UTC
            </span>
          </div>
        </div>

        <div style={{ padding: "24px 32px" }}>
          {loading ? SPINNER : (
            <>
              {/* Stat cards */}
              <div style={{ display: "flex", gap: 16, marginBottom: 32, flexWrap: "wrap" }}>
                <StatCard label="Total Transactions (1h)" value={stats?.total_transactions} color="#3182ce" />
                <StatCard label="Flagged (1h)"            value={stats?.flagged_count}      color="#e53e3e" />
                <StatCard label="Avg Fraud Score (1h)"    value={stats?.avg_fraud_score?.toFixed(1)} color="#dd6b20" />
                <StatCard label="Flag Rate (1h)"
                  value={stats?.total_transactions
                    ? ((stats.flagged_count / stats.total_transactions) * 100).toFixed(1) + "%"
                    : "—"}
                  color="#e94560" />
              </div>

              {/* Chart + Top Users side by side */}
              <div style={{ display: "flex", gap: 24, marginBottom: 32, flexWrap: "wrap" }}>
                <div style={{ flex: "2 1 400px", background: "#fff", border: "1px solid #e8e8e8",
                              borderRadius: 8, padding: 20, boxShadow: "0 1px 4px rgba(0,0,0,0.06)" }}>
                  <div style={{ color: "#999", fontSize: 11, textTransform: "uppercase",
                                letterSpacing: 1, marginBottom: 16 }}>
                    Recent Fraud Scores
                  </div>
                  {scores.length === 0
                    ? <div style={{ color: "#ccc", textAlign: "center", padding: 40 }}>No alerts yet</div>
                    : <ScoreChart scores={scores} />
                  }
                </div>

                <div style={{ flex: "1 1 200px", background: "#fff", border: "1px solid #e8e8e8",
                              borderRadius: 8, padding: 20, boxShadow: "0 1px 4px rgba(0,0,0,0.06)" }}>
                  <div style={{ color: "#999", fontSize: 11, textTransform: "uppercase",
                                letterSpacing: 1, marginBottom: 16 }}>
                    Top Risky Users (24h)
                  </div>
                  {topUsers.length === 0
                    ? <div style={{ color: "#ccc", fontSize: 13 }}>No data</div>
                    : topUsers.map((u, i) => (
                      <div key={u.user_id} style={{
                        display: "flex", justifyContent: "space-between", alignItems: "center",
                        padding: "8px 0", borderBottom: i < topUsers.length - 1 ? "1px solid #f0f0f0" : "none"
                      }}>
                        <span style={{ color: "#1a1a2e", fontSize: 13, fontWeight: 500 }}>{u.user_id}</span>
                        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
                          <span style={{ color: "#777", fontSize: 12 }}>{u.flagged_count} flags</span>
                          <ScoreBadge score={u.max_score} />
                        </div>
                      </div>
                    ))
                  }
                </div>
              </div>

              {/* Fraud table */}
              <div style={{ background: "#fff", border: "1px solid #e8e8e8", borderRadius: 8,
                            boxShadow: "0 1px 4px rgba(0,0,0,0.06)" }}>
                <div style={{ padding: "16px 20px", borderBottom: "1px solid #f0f0f0",
                              display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                  <span style={{ color: "#555", fontSize: 12, textTransform: "uppercase", letterSpacing: 1 }}>
                    Recent Fraud Alerts
                  </span>
                  <span style={{ color: "#888", fontSize: 12 }}>Auto-refreshes every 5s</span>
                </div>
                {scores.length === 0
                  ? <div style={{ color: "#ccc", textAlign: "center", padding: 40, fontSize: 13 }}>
                      No fraud alerts detected yet
                    </div>
                  : <FraudTable scores={scores} />
                }
              </div>
            </>
          )}
        </div>
      </div>
    </>
  );
}
JSXEOF

cat > $REPO_ROOT/k8s-apps/fraud-ui/index.html <<'EOF'
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Fraud Detection Dashboard</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.jsx"></script>
  </body>
</html>
EOF

cat > $REPO_ROOT/k8s-apps/fraud-ui/src/main.jsx <<'EOF'
import React from "react";
import { createRoot } from "react-dom/client";
import App from "./App.jsx";

createRoot(document.getElementById("root")).render(<App />);
EOF

cat > $REPO_ROOT/k8s-apps/fraud-ui/package.json <<'EOF'
{
  "name": "fraud-ui",
  "version": "1.0.0",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "preview": "vite preview"
  },
  "dependencies": {
    "react": "18.2.0",
    "react-dom": "18.2.0",
    "recharts": "2.12.7"
  },
  "devDependencies": {
    "@vitejs/plugin-react": "4.2.1",
    "vite": "5.2.0"
  }
}
EOF

cat > $REPO_ROOT/k8s-apps/fraud-ui/vite.config.js <<'EOF'
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api": "http://backend-api:8000"
    }
  }
});
EOF
```

### 8.2 Dockerfile + BuildKit Template (UI)

```bash
cat > $REPO_ROOT/k8s-apps/fraud-ui/Dockerfile <<'EOF'
FROM node:20-alpine AS builder
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
ENV NGINX_ENVSUBST_FILTER=API_KEY
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/templates/default.conf.template
RUN sed -i 's|pid\s*/run/nginx.pid;|pid /tmp/nginx.pid;|' /etc/nginx/nginx.conf \
    && chown -R 101:101 /var/cache/nginx /etc/nginx/conf.d
EXPOSE 8080
EOF

cat > $REPO_ROOT/k8s-apps/fraud-ui/nginx.conf <<'EOF'
server {
    listen 8080;
    root /usr/share/nginx/html;
    index index.html;

    location /api/ {
        proxy_pass http://backend-api.apps.svc.cluster.local:8000/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Api-Key ${API_KEY};
        proxy_connect_timeout 5s;
        proxy_read_timeout    10s;
        proxy_send_timeout    5s;
    }

    location / {
        try_files $uri $uri/ /index.html;
    }
}
EOF

cat > $REPO_ROOT/k8s-apps/fraud-ui/buildkit-job.yaml.tpl <<'EOF'
apiVersion: batch/v1
kind: Job
metadata:
  name: buildkit-fraud-ui-${GIT_SHA}
  namespace: bigdata
spec:
  backoffLimit: 1
  template:
    spec:
      restartPolicy: Never
      serviceAccountName: buildkit
      imagePullSecrets:
        - name: ghcr-creds
      initContainers:
        - name: copy-context
          image: busybox:1.36
          command: ['cp', '-r', '/src/.', '/workspace/']
          volumeMounts:
            - name: src
              mountPath: /src
            - name: workspace
              mountPath: /workspace
      containers:
        - name: buildctl
          image: moby/buildkit:v0.13.2
          command:
            - buildctl
            - --addr
            - tcp://buildkitd.bigdata.svc.cluster.local:1234
            - build
            - --frontend=dockerfile.v0
            - --local
            - context=/workspace
            - --local
            - dockerfile=/workspace
            - --output
            - type=image,name=ghcr.io/${ORG}/fraud-ui:${GIT_SHA},push=true
          volumeMounts:
            - name: workspace
              mountPath: /workspace
            - name: ghcr-secret
              mountPath: /kaniko/.docker
              readOnly: true
          env:
            - name: DOCKER_CONFIG
              value: /kaniko/.docker
      volumes:
        - name: workspace
          emptyDir: {}
        - name: src
          hostPath:
            path: ${HOST_UI_ROOT}
            type: Directory
        - name: ghcr-secret
          secret:
            secretName: ghcr-creds
            items:
              - key: .dockerconfigjson
                path: config.json
EOF

cat > $REPO_ROOT/k8s-apps/fraud-ui/fraud-ui.yaml.tpl <<'EOF'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fraud-ui
  namespace: apps
spec:
  replicas: 1
  selector:
    matchLabels:
      app: fraud-ui
  template:
    metadata:
      labels:
        app: fraud-ui
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 101
        fsGroup: 101
      imagePullSecrets:
        - name: ghcr-creds
      containers:
        - name: fraud-ui
          image: ghcr.io/${ORG}/fraud-ui:${GIT_SHA}
          imagePullPolicy: Always
          env:
            - name: API_KEY
              valueFrom:
                secretKeyRef:
                  name: api-key-secret
                  key: api-key
          ports:
            - containerPort: 8080
          livenessProbe:
            httpGet:
              path: /
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /
              port: 8080
            initialDelaySeconds: 3
            periodSeconds: 5
          resources:
            requests:
              cpu: "50m"
              memory: "64Mi"
            limits:
              cpu: "200m"
              memory: "128Mi"
---
apiVersion: v1
kind: Service
metadata:
  name: fraud-ui
  namespace: apps
spec:
  selector:
    app: fraud-ui
  ports:
    - port: 80
      targetPort: 8080
EOF
```

### 8.3 Build + Deploy UI

```bash
set -euo pipefail

mkdir -p "$HOST_UI_ROOT/_rendered"

# Generate package-lock.json (pins all transitive deps for reproducible npm ci)
cd "$HOST_UI_ROOT"
npm install --package-lock-only
cd "$REPO_ROOT"

# Build UI image
out_build="$HOST_UI_ROOT/_rendered/buildkit-fraud-ui.${GIT_SHA}.yaml"
envsubst < "$HOST_UI_ROOT/buildkit-job.yaml.tpl" > "$out_build"

kubectl -n bigdata delete job buildkit-fraud-ui-${GIT_SHA} \
  --ignore-not-found || true
kubectl -n bigdata apply -f "$out_build"
kubectl -n bigdata wait \
  --for=condition=complete \
  job/buildkit-fraud-ui-${GIT_SHA} --timeout=900s

# Deploy UI
out_deploy="$HOST_UI_ROOT/_rendered/fraud-ui.${GIT_SHA}.yaml"
envsubst < "$HOST_UI_ROOT/fraud-ui.yaml.tpl" > "$out_deploy"
kubectl -n apps apply -f "$out_deploy"
kubectl -n apps rollout status deploy/fraud-ui --timeout=180s
```

### 8.4 Ingress (same single-IP Hetzner pattern as Cassandra lab)

```bash
# Install ingress-nginx if not present
kubectl get ns ingress-nginx 2>/dev/null || \
  kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.11.3/deploy/static/provider/cloud/deploy.yaml

kubectl -n ingress-nginx rollout status \
  deploy/ingress-nginx-controller --timeout=180s

# Bind to host network (single-IP Hetzner pattern) — strategic merge is idempotent on re-run
kubectl -n ingress-nginx patch deploy ingress-nginx-controller \
  --type=strategic --patch '{"spec":{"template":{"spec":{"hostNetwork":true,"dnsPolicy":"ClusterFirstWithHostNet","containers":[{"name":"controller","ports":[{"containerPort":80,"hostPort":80,"protocol":"TCP"},{"containerPort":443,"hostPort":443,"protocol":"TCP"}]}]}}}}'

kubectl -n ingress-nginx rollout status \
  deploy/ingress-nginx-controller --timeout=180s

# Ingress for UI
cat > $REPO_ROOT/k8s-apps/apps-ingress-ui.yaml <<'EOF'
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: fraud-ui-ingress
  namespace: apps
spec:
  ingressClassName: nginx
  rules:
    - http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: fraud-ui
                port:
                  number: 80
EOF

# NOTE: No separate backend ingress — the UI nginx proxy injects X-Api-Key on /api/ requests.
# A direct /api ingress would bypass the key injection and return 401 to the browser.
kubectl apply -f $REPO_ROOT/k8s-apps/apps-ingress-ui.yaml

kubectl get ingress -n apps
```

### 8.5 Validate Phase 8

```bash
# Health check via public IP
curl -sS http://95.217.112.184/api/health

# UI is live
echo "Dashboard: http://95.217.112.184"
```

### 8.6 Commit Phase 8 to GitHub

```bash
cd $REPO_ROOT
git add k8s-apps/fraud-ui/package-lock.json
git add k8s-apps/fraud-ui/src/App.jsx
git add k8s-apps/fraud-ui/src/main.jsx
git add k8s-apps/fraud-ui/index.html
git add k8s-apps/fraud-ui/package.json
git add k8s-apps/fraud-ui/vite.config.js
git add k8s-apps/fraud-ui/Dockerfile
git add k8s-apps/fraud-ui/nginx.conf
git add k8s-apps/fraud-ui/buildkit-job.yaml.tpl
git add k8s-apps/fraud-ui/fraud-ui.yaml.tpl
git add k8s-apps/apps-ingress-ui.yaml
git commit -m "Phase 8: React fraud dashboard + Nginx + Ingress"
git push origin main
```

---

## ════════════════════════════════════════
## PHASE 9 — ARGOCD GITOPS
## ════════════════════════════════════════

> ArgoCD is a GitOps controller for Kubernetes. It watches a git repo and ensures
> the cluster state always matches what's in git — if someone manually changes a
> resource in the cluster, ArgoCD detects the drift and reverts it back to what
> git says. In plain terms: instead of running kubectl apply manually, you push
> to git and ArgoCD applies it for you automatically.
>
> In Huanca, ArgoCD polls gitops/bigdata/ on the main branch every 3 minutes and
> compares what's in git against what's running in the bigdata namespace.
>
> The flow:
> 1. You push a change to gitops/bigdata/ → ArgoCD pulls it → applies to cluster
> 2. Someone does a manual kubectl edit on a resource in bigdata namespace →
>    ArgoCD detects the diff against git → reverts it back
>
> ArgoCD never watches the cluster directly for changes — it always uses git as
> the source of truth and reconciles the cluster toward it. The cluster is
> downstream of git, not the other way around.
>
> The boundary is intentional: ArgoCD manages infrastructure that must converge to
> a known declared state. It does not manage application deployments in the apps
> namespace — those have their own independent release cycle and are managed
> imperatively through the deployment pipeline. Mixing the two would create
> operational conflict.
>
> SparkApplication is also excluded — it is a live streaming job that requires
> manual operational control (start, stop, restart, debug) and must never be
> auto-reverted by a GitOps controller.

### ArgoCD Scope

| Resource | Managed By | Reason |
|---|---|---|
| Redpanda cluster + topics | ArgoCD | Long-running infrastructure, declarative |
| StarRocks | ArgoCD | Long-running infrastructure, declarative |
| MinIO | ArgoCD | Long-running infrastructure, declarative |
| Airflow deployment | ArgoCD | Long-running infrastructure, declarative |
| Enrichment ConfigMap (customer CSV) | ArgoCD | Static reference data, declarative |
| SparkApplication (fraud-stream) | Imperative (`kubectl apply`) | Streaming job — needs manual start/stop/restart for ops and debug |
| Airflow DAGs | Airflow git-sync / ConfigMap | Business logic — Airflow manages execution schedule, not ArgoCD |
| fraud-ui Deployment | Imperative (BuildKit + envsubst) | SHA-tagged image requires manual build + deploy cycle |
| backend-api Deployment | Imperative (BuildKit + envsubst) | SHA-tagged image requires manual build + deploy cycle |

### 9.1 Install ArgoCD (idempotent)

```bash
kubectl create namespace argocd --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -n argocd \
  -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.13.3/manifests/install.yaml

kubectl -n argocd rollout status \
  deploy/argocd-server --timeout=300s
```

### 9.2 ArgoCD Repo Secret (SSH)

```bash
# Register the Huanca repo with ArgoCD using the existing SSH key
kubectl -n argocd create secret generic huanca-repo \
  --from-literal=type=git \
  --from-literal=url=git@github.com:jjcorderomejia/Huanca.git \
  --from-file=sshPrivateKey=$HOME/.ssh/github_huanca \
  --dry-run=client -o yaml | \
  kubectl label --local -f - argocd.argoproj.io/secret-type=repository -o yaml | \
  kubectl apply -f -
```

### 9.3 ArgoCD Application Manifest

```bash
mkdir -p $REPO_ROOT/gitops/argocd

cat > $REPO_ROOT/gitops/argocd/app-bigdata.yaml <<'EOF'
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: bigdata-fraud-lab
  namespace: argocd
spec:
  project: default
  source:
    repoURL: git@github.com:jjcorderomejia/Huanca.git
    targetRevision: main
    path: gitops/bigdata
  destination:
    server: https://kubernetes.default.svc
    namespace: bigdata
  syncPolicy:
    automated:
      prune: true
      selfHeal: true
    syncOptions:
      - CreateNamespace=true
      - ServerSideApply=true
EOF

kubectl apply -f $REPO_ROOT/gitops/argocd/app-bigdata.yaml
```

### 9.4 Validate Phase 9

```bash
# ArgoCD status
kubectl -n argocd get applications

# Resources synced
kubectl -n bigdata get kafkatopic 2>/dev/null || \
  kubectl -n bigdata get topic 2>/dev/null

kubectl -n bigdata get pods | grep -E "starrocks|minio|redpanda|airflow"

# FIX 14: Validate ArgoCD via ClusterIP — no port-forward
ARGO_SVC_IP=$(kubectl -n argocd get svc argocd-server \
  -o jsonpath='{.spec.clusterIP}')
curl -sk "https://${ARGO_SVC_IP}/api/version" | python3 -m json.tool
echo "✅ ArgoCD healthy at ClusterIP ${ARGO_SVC_IP}"

# Verify Application is Synced and Healthy
kubectl -n argocd get application bigdata-fraud-lab \
  -o jsonpath='{.status.sync.status}{" "}{.status.health.status}{"\n"}'
# Expected: Synced Healthy

# Confirm initial admin secret exists (do not print value)
kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.metadata.name}"
```

### 9.5 Commit Phase 9 to GitHub

```bash
cd $REPO_ROOT
git add gitops/argocd/app-bigdata.yaml
git commit -m "Phase 9: ArgoCD GitOps wiring"
git push origin main
```

---

## ════════════════════════════════════════
## PHASE 10 — END-TO-END SMOKE TESTS
## ════════════════════════════════════════

### 10.0 360° End-to-End Trace Plan

A unique `TRACE_UID` is injected in step 10.2 and explicitly traced (🔍) through
4 service tiers. Remaining steps are infrastructure-level assertions.
All steps must PASS before the final commit.

| Step  | Service              | Assertion                                       | TRACE_UID |
|-------|----------------------|-------------------------------------------------|-----------|
| 10.1  | —                    | API_KEY and TRACE_UID loaded                    |           |
| 10.2  | FastAPI Backend      | HTTP 200 + JSON from API Gateway                | 🔍        |
| 10.3  | Redpanda             | Malformed record injected into transactions-raw |           |
| 10.4  | Spark                | 30 s — pipeline processes both records          |           |
| 10.5  | Redpanda             | TRACE_UID found in transactions-raw consume     | 🔍        |
| 10.5  | Redpanda             | DLQ and fraud-alerts watermark > 0              |           |
| 10.6  | StarRocks            | TRACE_UID count > 0 in transactions table       | 🔍        |
| 10.7  | Spark + MinIO        | Micro-batch logs + checkpoints visible          |           |
| 10.8  | Iceberg + MinIO      | Warehouse file count > 0                        |           |
| 10.9  | FastAPI + Dashboard  | TRACE_UID found in fraud-scores response        | 🔍        |
| 10.10 | Airflow              | Fraud DAG registered                            |           |
| 10.11 | GitHub               | Milestone commit pushed                         |           |

### 10.1 Environment Setup

```bash
set -euo pipefail

source ~/.lab_Huanca

API_KEY=$(kubectl -n apps get secret api-key-secret \
  -o jsonpath='{.data.api-key}' | base64 -d)
echo "API_KEY loaded: ${#API_KEY} chars"

TRACE_UID="user-001"  # Existing customer: avg_amount_30d=150.00 — enables z-score rule
echo "TRACE_UID=${TRACE_UID}"
```

### 10.2 Send Trace Transaction (360° Anchor)

`TRACE_UID=user-001` is an existing customer (avg_amount_30d=150.00) so z-score can fire.
Strategy: 10 velocity-priming txns (count=10) + 1 high-amount trace txn (count=11 → velocity 40pts;
amount=500 → z-score=3.67 > 3.0 → 30pts). Total fraud_score = 70 ≥ 60 cutoff.
This UID is traced through Redpanda, StarRocks, and the Dashboard API.

```bash
# Velocity priming — 10 small txns to push count past 10 in the 5-min window
for i in $(seq 1 10); do
  curl -sS -X POST http://95.217.112.184/api/transaction \
    -H "Content-Type: application/json" \
    -H "X-Api-Key: ${API_KEY}" \
    -d "{\"user_id\":\"${TRACE_UID}\",\"amount\":10.00,\"merchant_id\":\"merchant-warm-${i}\",\"merchant_lat\":35.6762,\"merchant_lon\":139.6503,\"status\":\"completed\"}" \
    > /dev/null
done
echo "10 velocity-priming transactions sent"

# Trace transaction — amount=500 triggers z-score (500-150)/(150*0.3)=3.67 > 3.0
# Combined: velocity(40) + z-score(30) = 70pts — fraud flagged
curl -sS -X POST http://95.217.112.184/api/transaction \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: ${API_KEY}" \
  -d "{
    \"user_id\": \"${TRACE_UID}\",
    \"amount\": 500.00,
    \"merchant_id\": \"merchant-trace-99\",
    \"merchant_lat\": 35.6762,
    \"merchant_lon\": 139.6503,
    \"status\": \"completed\"
  }" | python3 -m json.tool

echo "Trace transaction sent — UID=${TRACE_UID} (velocity=11, z-score=3.67)"
```

### 10.3 DLQ Negative Test

Inject a malformed record directly into `transactions-raw` via `rpk produce`, bypassing
the backend's schema validation. Spark must route it to `transactions-dlq`.

```bash
echo '{"malformed":true,"missing_required_fields":1}' | \
  kubectl -n bigdata exec -i fraud-redpanda-0 -- \
  rpk topic produce transactions-raw
echo "Malformed record injected into transactions-raw"
```

### 10.4 Wait for Spark Micro-Batch

```bash
echo "Waiting 30 s for Spark micro-batch to process..."
sleep 30
echo "Ready."
```

### 10.5 Validate Kafka (Redpanda) Events

```bash
# 360° trace assertion — TRACE_UID must appear in transactions-raw
# -o -200 reads the last 200 messages; timeout 10 prevents hanging on low-volume topics
RAW_TRACE=$(timeout 10 kubectl -n bigdata exec fraud-redpanda-0 -- \
  rpk topic consume transactions-raw -o -200 2>/dev/null \
  | grep -c "${TRACE_UID}" || true)
if [ "${RAW_TRACE:-0}" -gt 0 ]; then
  echo "PASS: TRACE_UID=${TRACE_UID} found in transactions-raw"
else
  echo "FAIL: TRACE_UID=${TRACE_UID} not found in transactions-raw"; exit 1
fi

# DLQ assertion — high watermark must be > 0 after malformed inject in step 10.3
DLQ_HW=$(kubectl -n bigdata exec fraud-redpanda-0 -- \
  rpk topic describe transactions-dlq -p \
  | awk 'NR>1 {sum+=$6} END {print sum+0}')
if [ "${DLQ_HW:-0}" -gt 0 ]; then
  echo "PASS: DLQ high watermark = ${DLQ_HW}"
else
  echo "FAIL: DLQ high watermark = 0 — check Spark DLQ routing"; exit 1
fi

# Fraud alerts assertion — high watermark must be > 0 (high-amount txn flagged)
ALERTS_HW=$(kubectl -n bigdata exec fraud-redpanda-0 -- \
  rpk topic describe fraud-alerts -p \
  | awk 'NR>1 {sum+=$6} END {print sum+0}')
if [ "${ALERTS_HW:-0}" -gt 0 ]; then
  echo "PASS: fraud-alerts high watermark = ${ALERTS_HW}"
else
  echo "FAIL: fraud-alerts high watermark = 0 — check Spark fraud detection"; exit 1
fi
```

### 10.6 Validate StarRocks

```bash
SR_PASS=$(kubectl -n bigdata get secret starrocks-credentials \
  -o jsonpath='{.data.root-password}' | base64 -d)

# Informational queries
kubectl -n bigdata exec -it starrocks-fe-0 -- \
  env MYSQL_PWD="${SR_PASS}" mysql -h 127.0.0.1 -P 9030 -u root <<SQL

USE fraud;

-- Total transactions
SELECT count(*) AS total FROM transactions;

-- TOP INTERVIEW QUERY: Top 5 riskiest users last hour
SELECT
  user_id,
  count(*)            AS total_txns,
  sum(is_flagged)     AS flagged_count,
  max(fraud_score)    AS max_score,
  avg(amount)         AS avg_amount
FROM transactions
WHERE event_time >= date_sub(now(), INTERVAL 1 HOUR)
GROUP BY user_id
ORDER BY flagged_count DESC, max_score DESC
LIMIT 5;

-- Fraud breakdown by reason
SELECT
  reasons,
  count(*) AS occurrences
FROM fraud_scores
WHERE flagged_at >= date_sub(now(), INTERVAL 1 HOUR)
GROUP BY reasons
ORDER BY occurrences DESC;

SQL

# 360° trace assertion — TRACE_UID count must be > 0
SR_TRACE=$(kubectl -n bigdata exec -i starrocks-fe-0 -- \
  env MYSQL_PWD="${SR_PASS}" mysql -h 127.0.0.1 -P 9030 -u root \
  --batch --skip-column-names -e \
  "USE fraud; SELECT count(*) FROM transactions WHERE user_id = '${TRACE_UID}';")
if [ "${SR_TRACE:-0}" -gt 0 ]; then
  echo "PASS: TRACE_UID=${TRACE_UID} found in StarRocks transactions (${SR_TRACE} row(s))"
else
  echo "FAIL: TRACE_UID=${TRACE_UID} not found in StarRocks — check Spark sink"; exit 1
fi

unset SR_PASS
```

### 10.7 Validate Spark Processing

```bash
# Driver logs (should show micro-batches)
kubectl -n bigdata logs spark-fraud-stream-driver --tail=50 | \
  grep -E "Batch|offset|records"

# Executor pods running
kubectl -n bigdata get pods | grep spark-fraud-stream

# MinIO checkpoints
MINIO_USER=$(kubectl -n bigdata get secret minio-secret -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)
MINIO_PASS=$(kubectl -n bigdata get secret minio-secret -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)
CKPT_COUNT=$(kubectl -n bigdata exec -i minio-0 -- \
  env MC_HOST_local="http://${MINIO_USER}:${MINIO_PASS}@localhost:9000" \
  mc ls --recursive local/checkpoints/fraud-stream-v2/ | wc -l)
if [ "${CKPT_COUNT}" -gt 0 ]; then
  echo "PASS: Spark checkpoint has ${CKPT_COUNT} file(s) in fraud-stream-v2"
else
  echo "FAIL: Spark checkpoint is empty"; exit 1
fi
```

### 10.8 Validate Iceberg

```bash
MINIO_USER=$(kubectl -n bigdata get secret minio-secret -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)
MINIO_PASS=$(kubectl -n bigdata get secret minio-secret -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)
FILE_COUNT=$(kubectl -n bigdata exec -i minio-0 -- \
  env MC_HOST_local="http://${MINIO_USER}:${MINIO_PASS}@localhost:9000" \
  mc ls --recursive local/iceberg/warehouse/fraud/ | wc -l)
if [ "${FILE_COUNT}" -gt 0 ]; then
  echo "PASS: Iceberg warehouse has ${FILE_COUNT} file(s)"
else
  echo "FAIL: Iceberg warehouse is empty"; exit 1
fi
```

### 10.9 Validate Dashboard API

```bash
# Stats endpoint
curl -sS http://95.217.112.184/api/stats \
  -H "X-Api-Key: ${API_KEY}" | python3 -m json.tool

# Fraud scores
curl -sS "http://95.217.112.184/api/fraud-scores?limit=5" \
  -H "X-Api-Key: ${API_KEY}" | python3 -m json.tool

# Top risky users
curl -sS http://95.217.112.184/api/top-risky-users \
  -H "X-Api-Key: ${API_KEY}" | python3 -m json.tool

# 360° trace assertion — TRACE_UID must appear in fraud-scores
TRACE_IN_API=$(curl -sS "http://95.217.112.184/api/fraud-scores?limit=100" \
  -H "X-Api-Key: ${API_KEY}" | python3 -c "
import sys, json
data = json.load(sys.stdin)
rows = data if isinstance(data, list) else data.get('fraud_scores', [])
print(len([r for r in rows if r.get('user_id') == '${TRACE_UID}']))
")
if [ "${TRACE_IN_API:-0}" -gt 0 ]; then
  echo "PASS: TRACE_UID=${TRACE_UID} found in Dashboard API (${TRACE_IN_API} row(s))"
else
  echo "FAIL: TRACE_UID=${TRACE_UID} not found in Dashboard API"; exit 1
fi

# UI
echo "Open browser: http://95.217.112.184"
```

### 10.10 Validate Airflow DAG

```bash
# Airflow 3.x uses deploy/airflow-api-server (not the webserver label from 2.x)
DAG_COUNT=$(kubectl -n bigdata exec deploy/airflow-api-server -- \
  airflow dags list 2>/dev/null | grep -c "fraud" || true)
if [ "${DAG_COUNT}" -gt 0 ]; then
  echo "PASS: ${DAG_COUNT} fraud DAG(s) registered in Airflow"
else
  echo "FAIL: no fraud DAGs found in Airflow — check DAG deployment"; exit 1
fi

kubectl -n bigdata exec deploy/airflow-api-server -- \
  airflow dags list 2>/dev/null | grep fraud
```

### 10.11 Final Commit to GitHub

```bash
cd $REPO_ROOT
git commit --allow-empty -m "Phase 10: End-to-end smoke tests passed — lab complete"
git push origin main
echo "Lab complete. Repo: https://github.com/jjcorderomejia/Huanca"
```

---

## ════════════════════════════════════════
## DOCS
## ════════════════════════════════════════

### docs/semantics.md — Interview Talk Track

```
KAFKA SEMANTICS (REDPANDA):
  - At-least-once delivery — Spark commits offsets only after micro-batch completes
  - Restart replays are possible and expected
  - Sink MUST be idempotent to handle replays safely

IDEMPOTENCY STRATEGY:
  - transaction_id = UUID4 from producer (stable across retries)
  - StarRocks PRIMARY KEY table → duplicate transaction_id = upsert, not insert
  - No double-counting, no double-billing, ever

CHECKPOINTS:
  - Live on MinIO (s3a://checkpoints/fraud-stream-v2)
  - Survive pod restarts, node failures, Spark Operator reschedules
  - failOnDataLoss=false → pipeline recovers from Redpanda topic truncation

DLQ:
  - Malformed records go to transactions-dlq — never silently dropped
  - Includes Kafka metadata (topic/partition/offset) for traceability
  - 14-day retention → full audit window

FRAUD SCORING:
  - Rule engine (no ML) — transparent, auditable, explainable
  - Thresholds configurable via env vars without redeployment
  - Airflow retrain DAG updates thresholds daily based on false positive rate

ICEBERG CATALOG:
  - JDBC catalog backed by Airflow's PostgreSQL
  - Provides atomic commits via SQL compare-and-swap
  - Safe for concurrent writers (streaming + compaction)
  - Data files live on MinIO — catalog is just the metadata pointer
```

### docs/system_design.md — 5,000 Events/sec Design

```
THROUGHPUT TARGET: 5,000 transactions/sec

REDPANDA:
  - 6 partitions on transactions-raw → 6 parallel consumers
  - Each partition handles ~833 events/sec
  - Retention 7 days → replay buffer for backfill

SPARK:
  - 2 executors × 2 cores = 4 parallel tasks
  - Micro-batch interval: 10 seconds
  - At 5k eps × 10s = 50k records/batch → well within limits
  - Increase executor instances to 4 for sustained 10k eps

STARROCKS:
  - PRIMARY KEY table distributed across 12 buckets
  - JDBC batch insert 5000 rows/batch
  - For higher throughput: use StarRocks Stream Load API instead of JDBC

MINIO CHECKPOINTS:
  - S3A multipart upload handles large checkpoint files
  - Checkpoint interval tied to micro-batch = every 10 seconds

BOTTLENECKS TO WATCH:
  - StarRocks BE memory (increase to 16Gi for production)
  - MinIO disk I/O (use fast SSD for production)
  - Spark executor GC (tune with -XX:+UseG1GC)

SRE SIGNALS:
  - Redpanda consumer lag (transactions-raw — offsets managed via Spark checkpoint in MinIO, not a consumer group)
  - StarRocks row count vs Redpanda offset delta (Airflow reconcile DAG)
  - Spark micro-batch duration > 30s = pipeline falling behind
  - DLQ rate > 1% = schema change upstream
```

### docs/queries.md — Interview Queries

```sql
-- 1. TOP INTERVIEW: Top 5 riskiest users last hour
SELECT
  user_id,
  count(*)         AS total_txns,
  sum(is_flagged)  AS flagged_count,
  max(fraud_score) AS max_score
FROM fraud.transactions
WHERE event_time >= date_sub(now(), INTERVAL 1 HOUR)
GROUP BY user_id
ORDER BY flagged_count DESC
LIMIT 5;

-- 2. Fraud rate over time (5-min buckets)
SELECT
  date_trunc('minute', event_time)    AS bucket,
  count(*)                             AS total,
  sum(is_flagged)                      AS flagged,
  round(sum(is_flagged)/count(*)*100,2) AS fraud_rate_pct
FROM fraud.transactions
WHERE event_time >= date_sub(now(), INTERVAL 1 HOUR)
GROUP BY 1
ORDER BY 1 DESC;

-- 3. Fraud breakdown by reason
SELECT reasons, count(*) AS hits
FROM fraud.fraud_scores
WHERE flagged_at >= date_sub(now(), INTERVAL 24 HOUR)
GROUP BY reasons
ORDER BY hits DESC;

-- 4. Total $ at risk (flagged transactions last 24h)
SELECT
  sum(amount) AS total_at_risk,
  count(*)    AS flagged_count
FROM fraud.transactions
WHERE is_flagged = TRUE
  AND event_time >= date_sub(now(), INTERVAL 24 HOUR);

-- 5. Users with velocity violations
SELECT user_id, max(velocity_5min) AS peak_velocity
FROM fraud.transactions
WHERE velocity_5min > 10
  AND event_time >= date_sub(now(), INTERVAL 1 HOUR)
GROUP BY user_id
ORDER BY peak_velocity DESC
LIMIT 10;
```

---

## ════════════════════════════════════════
## QUICK REFERENCE CHEATSHEET
## ════════════════════════════════════════

```bash
# Check everything is running
kubectl -n bigdata get pods
kubectl -n apps    get pods
kubectl -n argocd  get applications

# Spark job status
kubectl -n bigdata get sparkapplication spark-fraud-stream

# Spark driver logs
kubectl -n bigdata logs -f spark-fraud-stream-driver --tail=100

# StarRocks quick query
SR_PASS=$(kubectl -n bigdata get secret starrocks-credentials \
  -o jsonpath='{.data.root-password}' | base64 -d)
kubectl -n bigdata exec -it starrocks-fe-0 -- \
  env MYSQL_PWD="${SR_PASS}" mysql -h 127.0.0.1 -P 9030 -u root \
  -e "SELECT count(*), sum(is_flagged) FROM fraud.transactions;"
unset SR_PASS

# Redpanda topic list
kubectl -n bigdata exec -it fraud-redpanda-0 -- rpk topic list

# MinIO bucket contents
MINIO_USER=$(kubectl -n bigdata get secret minio-secret -o jsonpath='{.data.MINIO_ROOT_USER}' | base64 -d)
MINIO_PASS=$(kubectl -n bigdata get secret minio-secret -o jsonpath='{.data.MINIO_ROOT_PASSWORD}' | base64 -d)
kubectl -n bigdata exec -i minio-0 -- \
  env MC_HOST_local="http://${MINIO_USER}:${MINIO_PASS}@localhost:9000" \
  mc ls local

# Send test transaction (API_KEY must be set — see Phase 10 step 10.0)
curl -sS -X POST http://95.217.112.184/api/transaction \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: ${API_KEY}" \
  -d '{"user_id":"test-001","amount":99.99,"merchant_id":"test-merchant","status":"completed"}'

# ArgoCD sync status
kubectl -n argocd get applications bigdata-fraud-lab

# Airflow DAG status
kubectl -n bigdata exec deploy/airflow-scheduler -- airflow dags list
```
