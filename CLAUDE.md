## Project Overview

**Huanca** is a Kubernetes-native real-time payment fraud detection lab (in progress).

Stack: Redpanda | Spark Structured Streaming | StarRocks | Apache Iceberg | MinIO | Airflow | ArgoCD | BuildKit | Terraform

## Session startup — required

At the start of every session, before responding to the user, run this exact command:

```bash
python3 << 'EOF'
import json, os

path = sorted(
    [os.path.expanduser(f'~/.claude/projects/-home-jjcm-Huanca/{f}')
     for f in os.listdir(os.path.expanduser('~/.claude/projects/-home-jjcm-Huanca/'))
     if f.endswith('.jsonl')],
    key=os.path.getmtime, reverse=True
)[0]

lines = open(path).readlines()
summary, messages = None, []

for line in reversed(lines):
    try:
        obj = json.loads(line.strip())
        if obj.get('subtype') == 'away_summary':
            summary = obj.get('content', '')
        role = obj.get('message', {}).get('role', '')
        content = obj.get('message', {}).get('content', '')
        if role in ('user', 'assistant'):
            text = next((c['text'] for c in content if isinstance(c, dict) and c.get('type') == 'text'), '') \
                   if isinstance(content, list) else content
            if text.strip():
                messages.append(f'[{role}]: {text[:400]}')
        if summary and len(messages) >= 4:
            break
    except:
        pass

if summary:
    print('=== SUMMARY ===\n' + summary[:1000])
for m in reversed(messages[:4]):
    print(m)
EOF
```

Parse the output. Do not use memory files as a substitute. If there is pending work or unresolved state, show the user a brief summary. If nothing is pending, proceed normally without mentioning the check.

## Working rules — strict

There is a runbook: `docs/FRAUD_LAB_COMPLETE_V9.md`. It is the single source of truth.

1. Before running any command, find it in the runbook first. If it is not in the runbook, stop and ask.
2. Always `source $HOST_HOME/.lab_Huanca` before any command. Each Bash tool call is a new shell — chain it at the start of every shell block: `source $HOST_HOME/.lab_Huanca && <command>`. This sets all required env vars (`GIT_SHA`, `REPO_ROOT`, `ORG`, etc.) and git identity. Never run `git config` manually — it is handled here.
3. Never run ad-hoc commands. Never chain with `&&` unless the runbook does.
4. Never directly edit `.tpl` or `.py` files. Always regenerate from the runbook's `cat >` heredoc.
5. When a file needs updating: (1) update runbook first, (2) find the runbook's heredoc that writes the file, (3) run that heredoc to regenerate the file on disk, (4) commit.
6. Only stage files that actually changed — not all files listed in a runbook `git add`.
7. Commit message: use the runbook's exact message only for first-deploy commits where the runbook defines one. For any subsequent fix or modification, generate a descriptive commit message at commit time — no runbook update required.
8. Never push `docs/` to git.
9. Always show the user the runbook's line numbers that will be modified before making any change.
10. Always show the user the line numbers in `docs/FRAUD_LAB_COMPLETE_V9.md` that match the proposed changes before executing anything.
11. When a blocking operation (wait, poll, build) stalls for 2 minutes, leave it running and open a parallel diagnostic: describe the resource, check logs, report findings — do not act until root cause is clear.
12. Never commit `**/_rendered/` — rendered manifests contain image SHAs and are generated at deploy time.
13. Never commit or stage build context copies — files copied into Docker build directories are ephemeral.
14. Never commit `**/*.tfstate`, `**/.terraform/`, or `**/*.tfvars` — Terraform state lives in MinIO; local plugin cache and var files stay local.
15. Never commit `*.env`, `*secret*`, or `*credentials*` files.
