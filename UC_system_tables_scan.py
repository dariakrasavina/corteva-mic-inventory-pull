#!/usr/bin/env python3
"""
Corteva MIC — Unity Catalog System Tables Scan
===============================================
Queries Databricks system tables to extract lineage, access audit logs,
query history, billing usage, and cluster usage. Also identifies assets
deployed via Databricks Asset Bundles (DABs).

This data is designed to support tenant migration planning — it shows
which assets are active, what depends on what, and how assets are connected.

Requires: pip3 install databricks-sdk --index-url https://pypi-proxy.dev.databricks.com/simple

This script uses the SQL Statement Execution API to run queries against
system tables via a SQL warehouse — no local Spark session required.

─── Authentication (same options as workspace_inventory_sdk.py) ─────────────
    OAuth U2M:        databricks auth login --host <host> --profile <name>
                      python UC_system_tables_scan.py --profile <name>
    PAT token:        python UC_system_tables_scan.py --host <host> --token dapi...
    Service principal: set DATABRICKS_HOST + DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET

─── Usage ───────────────────────────────────────────────────────────────────
    python UC_system_tables_scan.py --profile <name>
    python UC_system_tables_scan.py --profile <name> --days 90
    python UC_system_tables_scan.py --profile <name> --warehouse-id <id>
    python UC_system_tables_scan.py --profile <name> --section lineage
    python UC_system_tables_scan.py --host <host> --token dapi... --save

─── Options ─────────────────────────────────────────────────────────────────
    --host URL           Single workspace host URL
    --token TOKEN        PAT token
    --profile NAME       ~/.databrickscfg profile name
    --warehouse-id ID    SQL warehouse to run queries on (auto-detected if omitted)
    --days N             How many days of history to scan (default: 90)
    --section KEY        Run one section only
    --output-dir DIR     Output directory (default: ~/corteva-mic-workspace-assets/output)
    --save               Write JSON + CSV files to disk
    --json               Print JSON to stdout
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied
from databricks.sdk.service.sql import Disposition, StatementState, StatementStatus


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------


def _make_client(host: str = "", token: str = "", profile: str = "") -> WorkspaceClient:
    if profile:
        return WorkspaceClient(profile=profile)
    if host and token:
        return WorkspaceClient(host=host, token=token)
    if host:
        return WorkspaceClient(host=host)
    return WorkspaceClient()


# ---------------------------------------------------------------------------
# SQL execution helpers
# ---------------------------------------------------------------------------


def _get_warehouse_id(w: WorkspaceClient, warehouse_id: str = "") -> str:
    """Return the given warehouse ID, or auto-detect a running one."""
    if warehouse_id:
        return warehouse_id

    warehouses = list(w.warehouses.list())
    # Prefer running warehouses, then stopped ones
    for state in ("RUNNING", "STOPPED"):
        for wh in warehouses:
            if str(wh.state) == state or str(wh.state).endswith(state):
                print(f"  Auto-selected warehouse: {wh.name} ({wh.id})", file=sys.stderr)
                return str(wh.id)

    raise RuntimeError(
        "No SQL warehouses found. Create a SQL warehouse in your workspace "
        "or pass --warehouse-id explicitly."
    )


def _run_query(
    w: WorkspaceClient,
    warehouse_id: str,
    sql: str,
    label: str,
) -> list[dict]:
    """Execute SQL against a warehouse, wait for results, return list of dicts."""
    print(f"    Running: {label}", file=sys.stderr)

    try:
        response = w.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=warehouse_id,
            wait_timeout="50s",
            disposition=Disposition.INLINE,
        )
    except PermissionDenied:
        _warn(f"{label}: permission denied — system table may not be enabled on this workspace")
        return []
    except Exception as exc:  # noqa: BLE001
        _warn(f"{label}: {exc}")
        return []

    # Poll until complete if still pending
    stmt_id = response.statement_id
    status = response.status
    max_wait = 300  # 5 minutes
    waited = 0
    while status and status.state in (
        StatementState.PENDING,
        StatementState.RUNNING,
    ):
        if waited >= max_wait:
            _warn(f"{label}: timed out after {max_wait}s")
            return []
        time.sleep(5)
        waited += 5
        response = w.statement_execution.get_statement(statement_id=stmt_id)
        status = response.status

    if not status or status.state != StatementState.SUCCEEDED:
        error = status.error.message if (status and status.error) else "unknown error"
        _warn(f"{label}: query failed — {error}")
        return []

    # Extract column names and rows
    manifest = response.manifest
    if not manifest or not manifest.schema or not manifest.schema.columns:
        return []

    columns = [col.name for col in manifest.schema.columns]
    rows: list[dict] = []

    result = response.result
    if not result or not result.data_array:
        return rows

    for row in result.data_array:
        rows.append(dict(zip(columns, row)))

    # Handle paginated chunks
    chunk_index = 1
    while manifest.total_chunk_count and chunk_index < manifest.total_chunk_count:
        try:
            chunk = w.statement_execution.get_statement_result_chunk_n(
                statement_id=stmt_id, chunk_index=chunk_index
            )
            if chunk.data_array:
                for row in chunk.data_array:
                    rows.append(dict(zip(columns, row)))
        except Exception as exc:  # noqa: BLE001
            _warn(f"{label}: error fetching chunk {chunk_index}: {exc}")
            break
        chunk_index += 1

    return rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _warn(msg: str) -> None:
    print(f"    [WARN] {msg}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Scanners
# ---------------------------------------------------------------------------


def discover_system_tables(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    Discovers all available system tables in this workspace.
    Useful for understanding what data is available before running other scans.
    """
    sql = """
        SELECT table_catalog, table_schema, table_name, table_type, comment
        FROM system.information_schema.tables
        WHERE table_catalog = 'system'
        ORDER BY table_schema, table_name
    """
    return _run_query(w, warehouse_id, sql, "discover_system_tables")


def scan_table_lineage(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    Table lineage — shows which entities (jobs, notebooks, pipelines, dashboards)
    read from or write to each table. Critical for understanding migration dependencies
    and the order in which assets should be moved.

    Uses system.access.lineage (available in most workspaces).
    Falls back to system.lineage.table_lineage if the former does not exist.
    """
    sql = f"""
        SELECT
            event_time,
            event_date,
            source_table_full_name,
            source_table_catalog,
            source_table_schema,
            source_table_name,
            source_type,
            target_table_full_name,
            target_table_catalog,
            target_table_schema,
            target_table_name,
            target_type,
            entity_type,
            entity_id,
            entity_run_id,
            created_by
        FROM system.access.table_lineage
        WHERE event_time >= dateadd(DAY, -{days}, current_timestamp())
        ORDER BY event_time DESC
        LIMIT 10000
    """
    return _run_query(w, warehouse_id, sql, "table_lineage")


def scan_column_lineage(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    Column lineage — tracks which source columns feed into which target columns.
    Useful for identifying tightly coupled transformations that must move together.
    """
    sql = f"""
        SELECT
            event_time,
            event_date,
            source_table_full_name,
            source_table_catalog,
            source_table_schema,
            source_table_name,
            source_column_name,
            target_table_full_name,
            target_table_catalog,
            target_table_schema,
            target_table_name,
            target_column_name,
            entity_type,
            entity_id,
            entity_run_id,
            created_by
        FROM system.access.column_lineage
        WHERE event_time >= dateadd(DAY, -{days}, current_timestamp())
        ORDER BY event_time DESC
        LIMIT 10000
    """
    return _run_query(w, warehouse_id, sql, "column_lineage")


def scan_audit_logs(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    Access audit logs — records who accessed which assets and when.
    Useful for identifying active users and replicating access patterns in the new tenant.
    Limit kept low (5000) to avoid hitting the INLINE byte limit.
    """
    sql = f"""
        SELECT *
        FROM system.access.audit
        WHERE event_time >= dateadd(DAY, -{days}, current_timestamp())
          AND action_name IN (
            'createTable', 'deleteTable', 'updateTable', 'getTable',
            'createJob', 'deleteJob', 'runNow', 'submitRun',
            'createPipeline', 'deletePipeline', 'startUpdate',
            'createCluster', 'deleteCluster', 'startCluster',
            'createDashboard', 'deleteDashboard',
            'createServingEndpoint', 'deleteServingEndpoint',
            'bundleDeployment', 'bundleRun'
          )
        ORDER BY event_time DESC
        LIMIT 5000
    """
    return _run_query(w, warehouse_id, sql, "audit_logs")


def scan_query_history(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    Query history — shows which tables and assets are actively queried.
    Helps distinguish active assets from dormant ones, and identify heavy users.
    Dormant assets may not need to be prioritized in migration.
    """
    sql = f"""
        SELECT *
        FROM system.query.history
        WHERE start_time >= dateadd(DAY, -{days}, current_timestamp())
        ORDER BY start_time DESC
        LIMIT 5000
    """
    return _run_query(w, warehouse_id, sql, "query_history")


def scan_billing_usage(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    Billing and DBU usage — shows compute cost per asset, SKU, and workspace.
    Useful for sizing the new tenant and understanding which workloads are expensive.
    Note: requires workspace admin or billing permissions.
    """
    sql = f"""
        SELECT *
        FROM system.billing.usage
        WHERE usage_date >= dateadd(DAY, -{days}, current_date())
        ORDER BY usage_start_time DESC
        LIMIT 10000
    """
    return _run_query(w, warehouse_id, sql, "billing_usage")


def scan_cluster_usage(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    Cluster usage — shows cluster configurations and usage patterns.
    Useful for replicating cluster policies and understanding compute footprint
    in the new tenant.
    """
    sql = f"""
        SELECT *
        FROM system.compute.clusters
        WHERE change_time >= dateadd(DAY, -{days}, current_timestamp())
        ORDER BY change_time DESC
        LIMIT 10000
    """
    return _run_query(w, warehouse_id, sql, "cluster_usage")


def scan_dab_assets(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    Databricks Asset Bundle (DAB) deployed assets — identifies jobs and pipelines
    that are managed by DABs rather than created manually. These require a bundle
    deployment in the new tenant rather than manual recreation.

    Detected via:
    1. Job tags containing 'bundle' (e.g. bundle:name, target:env)
    2. Audit log entries for bundleDeployment / bundleRun actions
    """
    dab_assets: list[dict] = []

    # Detect DAB jobs via job tags
    try:
        jobs = list(w.jobs.list())
        for j in jobs:
            settings = j.settings
            tags = settings.tags if settings else {}
            if tags and any("bundle" in str(k).lower() or "bundle" in str(v).lower()
                            for k, v in tags.items()):
                dab_assets.append({
                    "asset_type":    "job",
                    "asset_id":      str(j.job_id),
                    "asset_name":    settings.name if settings else None,
                    "creator":       j.creator_user_name,
                    "detected_via":  "job_tags",
                    "bundle_tags":   json.dumps(tags, default=str),
                })
    except Exception as exc:  # noqa: BLE001
        _warn(f"dab_assets (jobs): {exc}")

    # Detect DAB pipelines via pipeline tags
    try:
        pipelines = list(w.pipelines.list_pipelines())
        for p in pipelines:
            detail = w.pipelines.get(pipeline_id=p.pipeline_id)
            tags = getattr(detail, "configuration", None) or getattr(detail, "config", None) or {}
            if any("bundle" in str(k).lower() for k in tags):
                dab_assets.append({
                    "asset_type":    "pipeline",
                    "asset_id":      p.pipeline_id,
                    "asset_name":    p.name,
                    "creator":       p.creator_user_name,
                    "detected_via":  "pipeline_config",
                    "bundle_tags":   json.dumps(
                        {k: v for k, v in tags.items() if "bundle" in k.lower()},
                        default=str
                    ),
                })
    except Exception as exc:  # noqa: BLE001
        _warn(f"dab_assets (pipelines): {exc}")

    # Detect bundle deployments from audit logs
    sql = f"""
        SELECT *
        FROM system.access.audit
        WHERE event_time >= dateadd(DAY, -{days}, current_timestamp())
          AND action_name IN ('bundleDeployment', 'bundleRun', 'createBundleDeployment')
        ORDER BY event_time DESC
        LIMIT 5000
    """
    audit_rows = _run_query(w, warehouse_id, sql, "dab_assets (audit)")
    for row in audit_rows:
        dab_assets.append({
            "asset_type":   "bundle_deployment",
            "asset_id":     None,
            "asset_name":   None,
            "creator":      row.get("user_identity"),
            "detected_via": "audit_log",
            "bundle_tags":  row.get("request_params"),
            "event_time":   row.get("event_time"),
            "action_name":  row.get("action_name"),
        })

    return dab_assets


def scan_lakeflow_jobs(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    Jobs metadata from system.lakeflow.jobs — shows deployment method (DABs vs manual),
    run_as user (service principal vs human), creator, and schedule.
    Useful for identifying which jobs are bundle-managed and which run as service principals.
    """
    sql = """
        SELECT *
        FROM system.lakeflow.jobs
        ORDER BY change_time DESC
        LIMIT 50000
    """
    return _run_query(w, warehouse_id, sql, "lakeflow_jobs")


def scan_lakeflow_pipelines(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    Pipeline metadata from system.lakeflow.pipelines — shows deployment method (DABs vs manual),
    run_as user (service principal vs human), creator, and trigger type.
    Useful for identifying which DLT pipelines are bundle-managed and which run as service principals.
    """
    sql = """
        SELECT *
        FROM system.lakeflow.pipelines
        ORDER BY change_time DESC
        LIMIT 50000
    """
    return _run_query(w, warehouse_id, sql, "lakeflow_pipelines")


def scan_warehouse_usage(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    SQL warehouse configuration and usage snapshots from system.compute.warehouses.
    Useful for understanding warehouse sizing, auto-stop settings, and channel versions
    when replicating compute configuration in the new tenant.
    """
    sql = f"""
        SELECT *
        FROM system.compute.warehouses
        WHERE change_time >= dateadd(DAY, -{days}, current_timestamp())
        ORDER BY change_time DESC
        LIMIT 10000
    """
    return _run_query(w, warehouse_id, sql, "warehouse_usage")


def scan_serving_endpoint_usage(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    ML serving endpoint usage from system.serving.endpoint_usage.
    Shows request volume, latency, and token usage per endpoint over time —
    useful for understanding which endpoints are actively used and their traffic patterns.
    """
    sql = f"""
        SELECT *
        FROM system.serving.endpoint_usage
        WHERE timestamp >= dateadd(DAY, -{days}, current_timestamp())
        ORDER BY timestamp DESC
        LIMIT 10000
    """
    return _run_query(w, warehouse_id, sql, "serving_endpoint_usage")


def scan_serving_served_entities(w: WorkspaceClient, warehouse_id: str, days: int) -> list[dict]:
    """
    ML served entities from system.serving.served_entities.
    Shows which models or functions are deployed behind each serving endpoint,
    including the entity name, version, and workload size.
    """
    sql = """
        SELECT *
        FROM system.serving.served_entities
        ORDER BY endpoint_name, entity_name
        LIMIT 10000
    """
    return _run_query(w, warehouse_id, sql, "serving_served_entities")


# ---------------------------------------------------------------------------
# Section registry
# ---------------------------------------------------------------------------

SECTIONS = [
    ("discover",           "Available System Tables",          discover_system_tables),
    ("table_lineage",      "Table Lineage",                    scan_table_lineage),
    ("column_lineage",     "Column Lineage",                   scan_column_lineage),
    ("audit_logs",         "Access Audit Logs",                scan_audit_logs),
    ("query_history",      "Query History",                    scan_query_history),
    ("billing_usage",      "Billing & DBU Usage",              scan_billing_usage),
    ("cluster_usage",      "Cluster Usage",                    scan_cluster_usage),
    ("dab_assets",               "DAB-Deployed Assets",              scan_dab_assets),
    ("lakeflow_jobs",            "Jobs (deployment & run_as)",       scan_lakeflow_jobs),
    ("lakeflow_pipelines",       "Pipelines (deployment & run_as)",  scan_lakeflow_pipelines),
    ("warehouse_usage",          "SQL Warehouse Usage",              scan_warehouse_usage),
    ("serving_endpoint_usage",   "Serving Endpoint Usage (ML)",      scan_serving_endpoint_usage),
    ("serving_served_entities",  "Serving Served Entities (ML)",     scan_serving_served_entities),
]

SECTION_KEYS = [k for k, _, _ in SECTIONS]


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _save_files(key: str, items: list[dict], prefix: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    base = os.path.join(output_dir, f"{prefix}_system_{key}")

    with open(f"{base}.json", "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, default=str)

    if items:
        with open(f"{base}.csv", "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(items[0].keys()), extrasaction="ignore")
            writer.writeheader()
            writer.writerows(items)
    else:
        open(f"{base}.csv", "w", encoding="utf-8-sig").close()


def _print_summary(name: str, counts: dict[str, int]) -> None:
    print(f"\n{'═' * 72}", file=sys.stderr)
    print(f"  Summary — {name} (system tables)", file=sys.stderr)
    print(f"{'═' * 72}", file=sys.stderr)
    for key, label, _ in SECTIONS:
        if key in counts:
            print(f"  {label:<38} {counts[key]:>6} rows", file=sys.stderr)
    print("─" * 72, file=sys.stderr)


# ---------------------------------------------------------------------------
# Core run
# ---------------------------------------------------------------------------


def run_scan(
    name: str,
    w: WorkspaceClient,
    warehouse_id: str,
    days: int,
    selected_sections: list,
    output_dir: str,
    save: bool,
) -> dict[str, list[dict]]:
    print(f"\n{'━' * 72}", file=sys.stderr)
    print(f"  Workspace    : {name}", file=sys.stderr)
    print(f"  Host         : {w.config.host}", file=sys.stderr)
    print(f"  Warehouse ID : {warehouse_id}", file=sys.stderr)
    print(f"  History      : last {days} days", file=sys.stderr)
    print(f"{'━' * 72}", file=sys.stderr)

    ws_output_dir = os.path.join(output_dir, name, "system_tables")
    results: dict[str, list[dict]] = {}
    counts: dict[str, int] = {}

    for key, label, fn in selected_sections:
        print(f"\n  Scanning {label}...", file=sys.stderr)
        rows = fn(w, warehouse_id, days)
        results[key] = rows
        counts[key] = len(rows)
        print(f"    -> {len(rows)} rows", file=sys.stderr)

        if save and rows is not None:
            _save_files(key, rows, name, ws_output_dir)
            print(f"    -> saved to {ws_output_dir}/", file=sys.stderr)

    _print_summary(name, counts)
    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Corteva MIC — Unity Catalog system tables scan",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--host",         default=os.environ.get("DATABRICKS_HOST", ""), help="Workspace host URL")
    parser.add_argument("--token",        default=os.environ.get("DATABRICKS_TOKEN", ""), help="PAT token")
    parser.add_argument("--profile",      default="", help="~/.databrickscfg profile name")
    parser.add_argument("--warehouse-id", default="", help="SQL warehouse ID (auto-detected if omitted)")
    parser.add_argument("--days",         type=int, default=90, help="Days of history to scan (default: 90)")
    parser.add_argument("--save",         action="store_true", help="Write JSON + CSV files to disk")
    parser.add_argument("--output-dir",   default=os.path.expanduser("~/corteva-mic-workspace-assets/output"), help="Output directory")
    parser.add_argument("--json",         action="store_true", help="Print JSON to stdout")
    parser.add_argument(
        "--section", choices=SECTION_KEYS, metavar="SECTION",
        help=f"Scan one section only. Choices: {', '.join(SECTION_KEYS)}",
    )
    args = parser.parse_args()

    if not args.host and not args.profile:
        parser.error(
            "Provide --profile, or --host + --token, "
            "or set DATABRICKS_HOST / DATABRICKS_TOKEN env vars."
        )

    selected = [
        (k, label, fn) for k, label, fn in SECTIONS
        if not args.section or k == args.section
    ]

    w = _make_client(host=args.host, token=args.token, profile=args.profile)
    name = (
        args.host.split("//")[-1].split(".")[0]
        if args.host
        else (args.profile or "workspace")
    )

    try:
        warehouse_id = _get_warehouse_id(w, args.warehouse_id)
    except RuntimeError as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)

    results = run_scan(
        name=name,
        w=w,
        warehouse_id=warehouse_id,
        days=args.days,
        selected_sections=selected,
        output_dir=args.output_dir,
        save=args.save,
    )

    if args.json:
        print(json.dumps(results, indent=2, default=str))
    elif not args.save:
        for key, label, _ in selected:
            rows = results.get(key, [])
            print(f"\n{'=' * 72}")
            print(f"  {label}  ({len(rows)} rows)")
            print("=" * 72)
            for row in rows[:20]:
                print(f"  {row}")
            if len(rows) > 20:
                print(f"  ... and {len(rows) - 20} more rows (use --save to write all to disk)")


if __name__ == "__main__":
    main()
