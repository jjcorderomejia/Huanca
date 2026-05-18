## Project Overview

**Huanca** is a Kubernetes-native real-time payment fraud detection lab (in progress).

Stack: Redpanda | Spark Structured Streaming | StarRocks | Apache Iceberg | MinIO | Airflow | ArgoCD | BuildKit | Terraform

## Working rules — strict

There is a runbook: `/home/jjcm/runbooks/huanca/FRAUD_LAB_COMPLETE_V9.md` (cloned from github.com/jjcorderomejia/runbooks). It is the single source of truth.
The runbooks repo lives at `/home/jjcm/runbooks/` — edit there, commit there, push there.

> **Note**: rules that apply across all jjcm's repos (no `Co-Authored-By`, no secret/env/credentials/Terraform-state commits, stage only changed files, descriptive commit messages, parallel diagnostic on long-running ops) live in `~/.claude/CLAUDE.md` (user-level, loaded into every Claude session in every project). The rules below are **Huanca-specific** — runbook-first workflow, heredoc regeneration, line-number citations, and the artifacts unique to this lab.

1. Before running any command, find it in the runbook first. If it is not in the runbook, stop and ask.
2. Always `source ${HOST_HOME:-$HOME}/.lab_Huanca` before any command. Each Bash tool call is a new shell — chain it at the start of every shell block: `source ${HOST_HOME:-$HOME}/.lab_Huanca && <command>`. This sets all required env vars (`GIT_SHA`, `REPO_ROOT`, `ORG`, etc.) and git identity. Never run `git config` manually — it is handled here.
3. Never run ad-hoc commands. Never chain with `&&` unless the runbook does.
4. Never directly edit `.tpl` or `.py` files. Always regenerate from the runbook's `cat >` heredoc.
5. When a file needs updating: (1) update runbook in `/home/jjcm/runbooks/` first and commit it, (2) find the runbook's heredoc that writes the file, (3) run that heredoc to regenerate the file on disk, (4) commit the regenerated file in the project repo.
6. Never push `docs/` to git.
7. Always show the user the runbook's line numbers that will be modified before making any change.
8. Always show the user the line numbers in `/home/jjcm/runbooks/huanca/FRAUD_LAB_COMPLETE_V9.md` that match the proposed changes before executing anything.
9. Never commit `**/_rendered/` — rendered manifests contain image SHAs and are generated at deploy time.
10. Never commit or stage build context copies — files copied into Docker build directories are ephemeral.
11. Runbook command execution procedure:
    a. In the Bash tool's `description` field, render the code exactly as it appears in the runbook with line numbers on the left side.
    b. In my text, only write the line range reference (e.g., "Lines XX-XX"). No separate code block.
    c. Do not ask for confirmation — the system's tool use permission prompt ("Do you want to proceed? ❯ Yes / No") is the only confirmation.
    d. The actual `command` field may use `&&` chaining for shell compatibility, but the `description` must show the runbook's original formatting.

## Browser-API features → check ingress-nginx first

Before adding any feature that uses: camera, microphone, geolocation,
clipboard, fullscreen, payment, USB, motion sensors, or iframe embedding —
check `cluster-config/manifests/ingress-nginx/values.yaml` (`addHeaders.Permissions-Policy`
and `X-Frame-Options`). The perimeter denies these by default. If a feature
needs one, decide: relax the perimeter, or set the header in the app's own
response. See `cluster-config/manifests/ingress-nginx/DESIGN.md` for context.
