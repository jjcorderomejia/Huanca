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

1. Before running any command, find it in the doc first. If it is not in the doc, stop and ask.
2. Always run `source $HOST_HOME/.lab_Huanca` at the start of every session before any command. This sets all required env vars (`GIT_SHA`, `REPO_ROOT`, `ORG`, etc.) and git identity (`user.email`, `user.name`). Never run `git config` manually — it is handled here.
3. Never run ad-hoc commands. Never chain with `&&` unless the doc does.
4. Never directly edit `.tpl` or `.py` files. Always regenerate from the doc's `cat >` heredoc.
5. When a file needs updating: (1) update doc first, (2) find the doc's heredoc that writes the file, (3) run that heredoc to regenerate the file on disk, (4) commit.
6. Only stage files that actually changed — not all files listed in a doc `git add`.
7. Commit message: use the doc's exact message only for first-deploy commits where the doc defines one. For any subsequent fix or modification, generate a descriptive commit message at commit time — no doc update required.
8. Never push `docs/` to git.
9. Always show the user the doc's line numbers that will be modified before making any change.
10. Always show the user the line numbers in `docs/FRAUD_LAB_COMPLETE_V9.md` that match the proposed changes before executing anything.
11. Each Bash tool call is a new shell — environment variables do not persist between calls.
12. `source $HOST_HOME/.lab_Huanca` must be chained at the start of every individual shell block: `source $HOST_HOME/.lab_Huanca && <command>`. Running it once in a prior call has no effect on subsequent calls.
13. When a blocking operation (wait, poll, build) shows no progress for 2 minutes, do not terminate it. Open a parallel diagnostic — describe the resource, check logs — and report findings before taking any action.
14. `**/_rendered/` — rendered manifests containing image SHAs; never commit.
15. Build context copies (files staged into Docker build directories) stay out of git — never commit or stage them.
16. `**/*.tfstate`, `**/.terraform/`, `**/*.tfvars` — Terraform state and local plugin cache; state lives in MinIO; never commit.
17. `*.env`, `*secret*`, `*credentials*` — all secret/env files; never commit.
