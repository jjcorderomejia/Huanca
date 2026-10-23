"""Shared helpers for the Huanca test suite.

Several production modules (the Spark jobs, the FastAPI backend) do real work at
import time: they build a SparkSession, open Kafka/StarRocks connections, or
start streaming queries. Rather than restructure production code for tests,
these helpers load only the pieces under test:

* ``load_symbols`` compiles selected top-level statements (functions, constants,
  imports) straight from a source file, so the code exercised is byte-for-byte
  the code that ships.
* ``find_assign`` / ``exec_statements`` pull specific statements (including ones
  nested inside a function body) for execution against local test data.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
SPARK_JOBS = REPO_ROOT / "spark-jobs"
SCORER_PATH = SPARK_JOBS / "fraud_stream_to_starrocks.py"
ICEBERG_WRITER_PATH = SPARK_JOBS / "fraud_iceberg_writer.py"
ICEBERG_SCHEMA_PATH = SPARK_JOBS / "init_iceberg_schema.py"
CUSTOMER_LOADER_PATH = SPARK_JOBS / "customer_csv_to_iceberg.py"
BACKEND_PATH = REPO_ROOT / "k8s-apps" / "backend-api" / "main.py"
GENERATOR_PATH = REPO_ROOT / "k8s-apps" / "txn-generator" / "generator.py"
DAGS_DIR = REPO_ROOT / "dags"
STARROCKS_DDL_PATH = REPO_ROOT / "gitops" / "bigdata" / "starrocks" / "init-ddl-job.yaml"
CUSTOMER_CSV_CONFIGMAP = REPO_ROOT / "gitops" / "bigdata" / "enrichment" / "customer-csv-configmap.yaml"


def parse(path: Path) -> ast.Module:
    return ast.parse(path.read_text(), filename=str(path))


def _assigned_names(node: ast.stmt) -> set[str]:
    targets: list[ast.expr] = []
    if isinstance(node, ast.Assign):
        targets = node.targets
    elif isinstance(node, ast.AnnAssign):
        targets = [node.target]
    return {t.id for t in targets if isinstance(t, ast.Name)}


def _import_allowed(node: ast.stmt, allowed_prefixes: Iterable[str]) -> bool:
    if isinstance(node, ast.ImportFrom):
        mod = node.module or ""
    elif isinstance(node, ast.Import):
        mod = node.names[0].name
    else:
        return False
    return any(mod == p or mod.startswith(p + ".") for p in allowed_prefixes)


def load_symbols(
    path: Path,
    names: Iterable[str],
    namespace: dict | None = None,
    import_prefixes: Iterable[str] = ("math", "json", "logging", "os", "time"),
) -> dict:
    """Execute only the named top-level defs/assignments (plus allowed imports)."""
    wanted = set(names)
    tree = parse(path)
    keep: list[ast.stmt] = []
    found: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            if _import_allowed(node, import_prefixes):
                keep.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name in wanted:
            keep.append(node)
            found.add(node.name)
        elif _assigned_names(node) & wanted:
            keep.append(node)
            found |= _assigned_names(node) & wanted
    missing = wanted - found
    if missing:
        raise LookupError(f"{path.name}: symbols not found: {sorted(missing)}")
    ns = {"__name__": f"huanca_under_test_{path.stem}"}
    if namespace:
        ns.update(namespace)
    exec(compile(ast.Module(body=keep, type_ignores=[]), str(path), "exec"), ns)
    return ns


def find_assign(path: Path, target: str, inside_function: str | None = None) -> ast.stmt:
    """Return the first assignment to ``target`` (optionally within a function)."""
    tree = parse(path)
    scope: list[ast.stmt] = tree.body
    if inside_function:
        fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == inside_function)
        scope = list(ast.walk(fn))  # type: ignore[assignment]
    for node in scope:
        if isinstance(node, ast.Assign) and target in _assigned_names(node):
            return node
    raise LookupError(f"{path.name}: no assignment to {target!r}")


def exec_statements(path: Path, stmts: list[ast.stmt], namespace: dict) -> dict:
    exec(compile(ast.Module(body=stmts, type_ignores=[]), str(path), "exec"), namespace)
    return namespace


# ── Schema extraction helpers (contract tests) ─────────────────────────
def structfield_names(path: Path, schema_var: str) -> list[str]:
    """Field names of a top-level ``X = StructType([StructField("name", ...), ...])``."""
    node = find_assign(path, schema_var)
    names = []
    for call in ast.walk(node.value):
        if isinstance(call, ast.Call) and getattr(call.func, "id", None) == "StructField":
            names.append(call.args[0].value)
    return names


def sql_create_columns(sql: str, table: str) -> list[str]:
    """Column names from ``CREATE TABLE IF NOT EXISTS <table> ( ... )``."""
    m = re.search(
        rf"CREATE TABLE IF NOT EXISTS\s+{re.escape(table)}\s*\((.*?)\n\s*\)",
        sql,
        flags=re.S,
    )
    if not m:
        raise LookupError(f"CREATE TABLE {table} not found")
    cols = []
    for line in m.group(1).splitlines():
        line = line.strip()
        if not line or line.startswith("--"):
            continue
        cols.append(line.split()[0].rstrip(","))
    return cols
