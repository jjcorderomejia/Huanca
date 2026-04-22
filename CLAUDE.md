# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Huanca** is a Kubernetes-native real-time payment fraud detection lab (in progress).

Stack: Redpanda | Spark Structured Streaming | StarRocks | Apache Iceberg | MinIO | Airflow | ArgoCD | BuildKit | Terraform

## Infrastructure — Terraform (`infra/terraform/`)

Terraform runs **inside a Kubernetes Job** (not locally). It targets an existing K8s cluster and manages only RBAC/ServiceAccounts — it never creates namespaces (those are pre-existing: `bigdata`, `apps`, `argocd`, `spark-operator`).

**Backend**: S3-compatible via MinIO at `minio.bigdata.svc.cluster.local:9000`, bucket `tf-state`, key `fraud-lab/terraform.tfstate`. Credentials come from env vars `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` injected from the `minio-secret` K8s secret — never hardcoded.

**Variables**: `ghcr_token` (sensitive, required) and `ghcr_user` are passed at plan/apply time. `.tfvars` files are gitignored and must never be committed.

### Common Terraform commands

```bash
# Init (run from inside the K8s Job context, or with KUBECONFIG set)
terraform -chdir=infra/terraform init

# Plan / Apply
terraform -chdir=infra/terraform plan -var="ghcr_token=<token>"
terraform -chdir=infra/terraform apply -var="ghcr_token=<token>"
```

## Working rules — strict

There is a master document: `docs/FRAUD_LAB_COMPLETE_V9.md`. It is the single source of truth.

- Before running any command, find it in the doc first. If it is not in the doc, stop and ask.
- Never run ad-hoc commands. Never chain with `&&` unless the doc does.
  - Always run `source $HOST_HOME/.lab_Huanca` at the start of every session before any command. This sets all required env vars (`GIT_SHA`, `REPO_ROOT`, `ORG`, etc.) and git identity (`user.email`, `user.name`). Never run `git config` manually — it is handled here.
- Never directly edit `.tpl` or `.py` files. Always regenerate from the doc's `cat >` heredoc.
- When a file needs updating: (1) update doc first, (2) find the doc's heredoc that writes the file, (3) run that heredoc to regenerate the file on disk, (4) commit.
- Only stage files that actually changed — not all files listed in a doc `git add`.
- Commit message: use the doc's exact message only for first-deploy commits where the doc defines one. For any subsequent fix or modification, generate a descriptive commit message at commit time — no doc update required.
- Never push `docs/` to git.
- Always show the user the doc's line numbers that will be modified before making any change.
- Always show the user the line numbers in `docs/FRAUD_LAB_COMPLETE_V9.md` that match the proposed changes before executing anything.

## Key `.gitignore` rules

- `**/_rendered/` — rendered manifests containing image SHAs; never commit
- `**/*.tfstate`, `**/.terraform/`, `**/*.tfvars` — Terraform state and local plugin cache; state lives in MinIO
- `*.env`, `*secret*`, `*credentials*` — all secret/env files
