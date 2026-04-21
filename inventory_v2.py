#!/usr/bin/env python3
"""
Corteva MIC — Workspace Inventory  (v2 — Databricks Python SDK)
================================================================
Uses the official Databricks Python SDK for authentication and API calls.

Install:  pip install databricks-sdk
          (Databricks employees: pip install databricks-sdk --index-url <internal-mirror>)

─── Authentication options ──────────────────────────────────────────────────
    PAT token:    --host URL --token dapi...
    CLI profile:  --profile my-profile   (reads ~/.databrickscfg)
    Env vars:     DATABRICKS_HOST + DATABRICKS_TOKEN (SDK picks up automatically)
    Default:      SDK auto-discovers from env / [DEFAULT] profile in ~/.databrickscfg

─── Single workspace ────────────────────────────────────────────────────────
    python inventory_v2.py --host https://adb-xxx.net --token dapi...
    python inventory_v2.py --profile my-profile
    python inventory_v2.py --profile my-profile --section jobs

─── Multiple workspaces (reads workspaces.json) ─────────────────────────────
    python inventory_v2.py --config workspaces.json

─── Options ─────────────────────────────────────────────────────────────────
    --config FILE       Path to workspaces JSON config (runs all workspaces)
    --host URL          Single workspace host URL
    --token TOKEN       Single workspace PAT token
    --profile NAME      ~/.databrickscfg profile name
    --save              Write JSON + CSV files to disk (always on with --config)
    --output-dir DIR    Root output directory (default: ./output)
    --section KEY       Collect one section only
    --json              Print JSON to stdout (single workspace only)

─── workspaces.json format — supports token or profile per workspace ─────────
    [
      {"name": "dev",  "host": "https://adb-xxx.net", "token": "dapi..."},
      {"name": "uat",  "host": "https://adb-yyy.net", "profile": "uat-profile"},
      {"name": "prod", "host": "https://adb-zzz.net", "profile": "prod-profile"}
    ]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------


def _make_client(host: str = "", token: str = "", profile: str = "") -> WorkspaceClient:
    """Create a WorkspaceClient using the best available auth method."""
    if profile:
        return WorkspaceClient(profile=profile)
    if host and token:
        return WorkspaceClient(host=host, token=token)
    if host:
        # SDK auto-discovers auth from env vars or default profile
        return WorkspaceClient(host=host)
    return WorkspaceClient()  # fully auto-discover


# ---------------------------------------------------------------------------
# Inventory collector — wraps WorkspaceClient, tracks per-section errors
# ---------------------------------------------------------------------------


class InventoryCollector:
    def __init__(self, w: WorkspaceClient) -> None:
        self.w = w
        self._perm_errors: list[str] = []
        self._other_errors: list[str] = []

    def safe(self, label: str, fn: Any, default: Any = None) -> Any:
        """Call fn(), catch SDK errors, return default on failure."""
        try:
            return fn()
        except PermissionDenied:
            self._perm_errors.append(label)
        except (NotFound, ResourceDoesNotExist):
            pass
        except Exception as exc:  # noqa: BLE001
            self._other_errors.append(f"{label}: {exc}")
            _warn(f"{label}: {exc}")
        return default

    def pop_errors(self) -> tuple[list[str], list[str]]:
        perm = list(self._perm_errors)
        other = list(self._other_errors)
        self._perm_errors.clear()
        self._other_errors.clear()
        return perm, other

    def _uc_iter_schemas(self) -> list[tuple[str, str]]:
        """Return all (catalog, schema) pairs visible to this token."""
        pairs: list[tuple[str, str]] = []
        catalogs = self.safe("catalogs.list", lambda: list(self.w.catalogs.list()), default=[])
        for cat in catalogs:
            cat_name = cat.name
            schemas = self.safe(
                f"schemas.list({cat_name})",
                lambda cn=cat_name: list(self.w.schemas.list(catalog_name=cn)),
                default=[],
            )
            for schema in schemas:
                pairs.append((cat_name, schema.name))
        return pairs


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _warn(msg: str) -> None:
    print(f"    [WARN] {msg}", file=sys.stderr)


def _fmt_ts(ms: int | float | None) -> str | None:
    if not ms:
        return None
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _fmt_dt(dt: datetime | str | None) -> str | None:
    """Format a datetime object or ISO string."""
    if dt is None:
        return None
    if isinstance(dt, datetime):
        return dt.strftime("%Y-%m-%d %H:%M UTC")
    try:
        return datetime.fromisoformat(str(dt).replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:  # noqa: BLE001
        return str(dt)


def _val(v: Any) -> Any:
    """Return the .value of an SDK enum, or the value as-is."""
    if v is None:
        return None
    if hasattr(v, "value"):
        return v.value
    return v


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------


def collect_jobs(c: InventoryCollector) -> list[dict]:
    raw = c.safe("jobs.list", lambda: list(c.w.jobs.list()), default=[])
    jobs = []
    for j in raw:
        jid = j.job_id

        detail = c.safe(f"jobs.get({jid})", lambda jid=jid: c.w.jobs.get(job_id=jid))
        settings = detail.settings if detail else None
        schedule = settings.schedule if settings else None
        trigger = settings.trigger if settings else None

        cron = schedule.quartz_cron_expression if schedule else None
        tz_id = schedule.timezone_id if schedule else None
        paused = _val(schedule.pause_status) if schedule else None

        runs = c.safe(
            f"jobs.list_runs({jid})",
            lambda jid=jid: list(c.w.jobs.list_runs(job_id=jid, limit=5)),
            default=[],
        )

        last_run_time = None
        last_run_state = None
        if runs:
            r = runs[0]
            last_run_time = _fmt_ts(r.start_time)
            if r.state:
                last_run_state = _val(r.state.result_state) or _val(r.state.life_cycle_state)

        if schedule and paused != "PAUSED":
            status = "SCHEDULED (active)"
        elif schedule and paused == "PAUSED":
            status = "SCHEDULED (paused)"
        elif trigger:
            status = "TRIGGERED"
        elif runs:
            status = "MANUAL (has runs)"
        else:
            status = "INACTIVE"

        jobs.append({
            "job_id":          jid,
            "name":            settings.name if settings else None,
            "creator":         j.creator_user_name,
            "status":          status,
            "is_scheduled":    bool(schedule),
            "schedule_cron":   cron,
            "schedule_tz":     tz_id,
            "schedule_paused": paused,
            "last_run_time":   last_run_time,
            "last_run_state":  last_run_state,
        })
    return jobs


def collect_pipelines(c: InventoryCollector) -> list[dict]:
    raw = c.safe("pipelines.list", lambda: list(c.w.pipelines.list_pipelines()), default=[])
    pipelines = []
    for p in raw:
        pid = p.pipeline_id

        detail = c.safe(f"pipelines.get({pid})", lambda pid=pid: c.w.pipelines.get(pipeline_id=pid))
        continuous = detail.continuous if detail else False
        trigger = detail.trigger if detail else None

        events = c.safe(
            f"pipelines.events({pid})",
            lambda pid=pid: list(c.w.pipelines.list_pipeline_events(pipeline_id=pid, max_results=5)),
            default=[],
        )
        last_event_time = _fmt_dt(events[0].timestamp) if events else None
        last_event_type = _val(events[0].event_type) if events else None

        if continuous:
            status = "CONTINUOUS"
        elif trigger:
            status = "TRIGGERED"
        elif last_event_time:
            status = "MANUAL (has runs)"
        else:
            status = "INACTIVE"

        pipelines.append({
            "pipeline_id":     pid,
            "name":            p.name,
            "state":           _val(p.state),
            "creator":         p.creator_user_name,
            "status":          status,
            "is_continuous":   bool(continuous),
            "is_triggered":    bool(trigger),
            "last_run_time":   last_event_time,
            "last_event_type": last_event_type,
        })
    return pipelines


def collect_notebooks(c: InventoryCollector) -> list[dict]:
    notebooks: list[dict] = []

    def recurse(path: str) -> None:
        objects = c.safe(
            f"workspace.list({path})",
            lambda p=path: list(c.w.workspace.list(path=p)),
            default=[],
        )
        for obj in objects:
            if _val(obj.object_type) == "NOTEBOOK":
                notebooks.append({
                    "object_id": obj.object_id,
                    "path":      obj.path,
                    "language":  _val(obj.language),
                })
            elif _val(obj.object_type) == "DIRECTORY" and obj.path:
                recurse(obj.path)

    recurse("/")
    return notebooks


def collect_tables(c: InventoryCollector) -> list[dict]:
    tables: list[dict] = []
    for cat, schema in c._uc_iter_schemas():
        rows = c.safe(
            f"tables.list({cat}.{schema})",
            lambda cn=cat, sn=schema: list(c.w.tables.list(catalog_name=cn, schema_name=sn)),
            default=[],
        )
        for t in rows:
            tables.append({
                "catalog_name":       t.catalog_name,
                "schema_name":        t.schema_name,
                "table_name":         t.name,
                "full_name":          t.full_name,
                "table_type":         _val(t.table_type),
                "data_source_format": _val(t.data_source_format),
                "storage_location":   t.storage_location,
                "owner":              t.owner,
                "created_at":         _fmt_ts(t.created_at),
                "created_by":         t.created_by,
                "updated_at":         _fmt_ts(t.updated_at),
                "updated_by":         t.updated_by,
                "comment":            t.comment,
            })
    return tables


def collect_volumes(c: InventoryCollector) -> list[dict]:
    volumes: list[dict] = []
    for cat, schema in c._uc_iter_schemas():
        rows = c.safe(
            f"volumes.list({cat}.{schema})",
            lambda cn=cat, sn=schema: list(c.w.volumes.list(catalog_name=cn, schema_name=sn)),
            default=[],
        )
        for v in rows:
            volumes.append({
                "catalog_name":     v.catalog_name,
                "schema_name":      v.schema_name,
                "volume_name":      v.name,
                "full_name":        v.full_name,
                "volume_type":      _val(v.volume_type),
                "storage_location": v.storage_location,
                "owner":            v.owner,
                "created_at":       _fmt_ts(v.created_at),
                "created_by":       v.created_by,
                "updated_at":       _fmt_ts(v.updated_at),
                "updated_by":       v.updated_by,
                "comment":          v.comment,
            })
    return volumes


def collect_functions(c: InventoryCollector) -> list[dict]:
    functions: list[dict] = []
    for cat, schema in c._uc_iter_schemas():
        rows = c.safe(
            f"functions.list({cat}.{schema})",
            lambda cn=cat, sn=schema: list(c.w.functions.list(catalog_name=cn, schema_name=sn)),
            default=[],
        )
        for fn in rows:
            functions.append({
                "catalog_name":       fn.catalog_name,
                "schema_name":        fn.schema_name,
                "function_name":      fn.name,
                "full_name":          fn.full_name,
                "data_type":          _val(fn.data_type),
                "routine_body":       _val(fn.routine_body),
                "routine_definition": fn.routine_definition,
                "language":           fn.external_language or _val(fn.routine_body),
                "owner":              fn.owner,
                "created_at":         _fmt_ts(fn.created_at),
                "created_by":         fn.created_by,
                "updated_at":         _fmt_ts(fn.updated_at),
                "updated_by":         fn.updated_by,
                "comment":            fn.comment,
            })
    return functions


def collect_genie_spaces(c: InventoryCollector) -> list[dict]:
    # w.genie is present in SDK >= 0.20; fall back to direct HTTP if unavailable
    spaces = c.safe(
        "genie.list_spaces",
        lambda: list(c.w.genie.list_spaces()),
        default=None,
    )
    if spaces is not None:
        return [
            {
                "space_id": getattr(s, "space_id", None) or getattr(s, "id", None),
                "title":    getattr(s, "title", None) or getattr(s, "name", None),
            }
            for s in spaces
        ]

    # Fallback via SDK's internal HTTP client
    try:
        data = c.w.api_client.do("GET", "/api/2.0/genie/spaces")
        return [
            {
                "space_id": s.get("id") or s.get("space_id"),
                "title":    s.get("title") or s.get("name"),
            }
            for s in (data.get("spaces") or [])
        ]
    except Exception as exc:  # noqa: BLE001
        c._other_errors.append(f"genie_spaces: {exc}")
        return []


def collect_experiments(c: InventoryCollector) -> list[dict]:
    exps = c.safe(
        "experiments.search",
        lambda: list(c.w.experiments.search_experiments()),
        default=[],
    )
    return [
        {
            "experiment_id":     e.experiment_id,
            "name":              e.name,
            "lifecycle_stage":   e.lifecycle_stage,
            "artifact_location": e.artifact_location,
        }
        for e in exps
    ]


def collect_dashboards(c: InventoryCollector) -> list[dict]:
    items: list[dict] = []

    # Lakeview dashboards
    lakeview = c.safe("lakeview.list", lambda: list(c.w.lakeview.list()), default=[])
    for d in lakeview:
        items.append({
            "dashboard_id": d.dashboard_id,
            "display_name": d.display_name,
            "path":         d.path,
            "type":         "lakeview",
        })

    # Classic (legacy) dashboards
    classic = c.safe("dashboards.list", lambda: list(c.w.dashboards.list()), default=[])
    for d in classic:
        items.append({
            "dashboard_id": d.id,
            "display_name": d.name,
            "path":         None,
            "type":         "classic",
        })

    return items


def collect_serving_endpoints(c: InventoryCollector) -> list[dict]:
    endpoints = c.safe(
        "serving_endpoints.list",
        lambda: list(c.w.serving_endpoints.list()),
        default=[],
    )
    return [
        {
            "name":                e.name,
            "creator":             e.creator,
            "state_ready":         _val(e.state.ready) if e.state else None,
            "state_config_update": _val(e.state.config_update) if e.state else None,
        }
        for e in endpoints
    ]


def collect_apps(c: InventoryCollector) -> list[dict]:
    apps = c.safe("apps.list", lambda: list(c.w.apps.list()), default=[])
    return [
        {
            "name":        a.name,
            "description": a.description,
            "status":      _val(a.status.state) if a.status else None,
            "url":         a.url,
        }
        for a in apps
    ]


def collect_repos(c: InventoryCollector) -> list[dict]:
    """Git folders / Repos — uses SDK repos.list() and workspace.list() fallback."""
    repos_raw = c.safe("repos.list", lambda: list(c.w.repos.list()), default=None)

    if repos_raw is not None:
        repos = []
        for r in repos_raw:
            path = r.path or ""
            owner = path.split("/")[2] if path.count("/") >= 2 else None
            repos.append({
                "repo_id":        r.id,
                "path":           path,
                "url":            r.url,
                "provider":       r.provider,
                "branch":         r.branch,
                "head_commit_id": r.head_commit_id,
                "owner":          owner,
            })
        return repos

    # Fallback: scan workspace paths for REPO objects
    def find_repos(path: str) -> list[Any]:
        found: list[Any] = []
        objects = c.safe(
            f"workspace.list({path})",
            lambda p=path: list(c.w.workspace.list(path=p)),
            default=[],
        )
        for obj in objects:
            if _val(obj.object_type) == "REPO":
                found.append(obj)
            elif _val(obj.object_type) == "DIRECTORY" and obj.path:
                found.extend(find_repos(obj.path))
        return found

    raw: list[Any] = []
    for root in ("/Users", "/Repos"):
        raw.extend(find_repos(root))

    repos = []
    for obj in raw:
        rid = obj.resource_id or obj.object_id
        detail = c.safe(
            f"repos.get({rid})",
            lambda rid=rid: c.w.repos.get(repo_id=rid),
        )
        path = (detail.path if detail else None) or obj.path or ""
        owner = path.split("/")[2] if path.count("/") >= 2 else None
        repos.append({
            "repo_id":        detail.id if detail else rid,
            "path":           path,
            "url":            detail.url if detail else None,
            "provider":       detail.provider if detail else None,
            "branch":         detail.branch if detail else None,
            "head_commit_id": detail.head_commit_id if detail else None,
            "owner":          owner,
        })
    return repos


def collect_registered_models(c: InventoryCollector) -> list[dict]:
    models: list[dict] = []

    # Unity Catalog models
    uc_models = c.safe(
        "registered_models.list (UC)",
        lambda: list(c.w.registered_models.list()),
        default=[],
    )
    for m in uc_models:
        models.append({
            "full_name":    m.full_name,
            "catalog_name": m.catalog_name,
            "schema_name":  m.schema_name,
            "name":         m.name,
            "owner":        m.owner,
            "type":         "unity_catalog",
        })

    # Legacy workspace model registry
    legacy = c.safe(
        "model_registry.list (legacy)",
        lambda: list(c.w.model_registry.list_registered_models()),
        default=[],
    )
    for m in legacy:
        models.append({
            "full_name":    None,
            "catalog_name": None,
            "schema_name":  None,
            "name":         m.name,
            "owner":        None,
            "type":         "workspace_registry",
        })

    return models


# ---------------------------------------------------------------------------
# Section registry
# ---------------------------------------------------------------------------

SECTIONS = [
    ("jobs",              "Jobs",                               collect_jobs),
    ("pipelines",         "DLT Pipelines",                     collect_pipelines),
    ("notebooks",         "Notebooks",                         collect_notebooks),
    ("tables",            "Tables (Unity Catalog)",            collect_tables),
    ("volumes",           "Volumes (Unity Catalog)",           collect_volumes),
    ("functions",         "Functions (Unity Catalog)",         collect_functions),
    ("genie_spaces",      "Genie Spaces",                      collect_genie_spaces),
    ("experiments",       "ML Experiments",                    collect_experiments),
    ("dashboards",        "Dashboards",                        collect_dashboards),
    ("serving_endpoints", "Serving Endpoints (agents/models)", collect_serving_endpoints),
    ("apps",              "Apps",                              collect_apps),
    ("repos",             "Repos / Git Folders",               collect_repos),
    ("registered_models", "Registered ML Models",              collect_registered_models),
]

SECTION_KEYS = [k for k, _, _ in SECTIONS]


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _print_section(label: str, items: list[dict]) -> None:
    sep = "=" * 72
    print(f"\n{sep}")
    print(f"  {label}  ({len(items)} found)")
    print(sep)
    if not items:
        print("  (none)")
        return
    for item in items:
        row = "  |  ".join(f"{k}: {v}" for k, v in item.items() if v is not None)
        print(f"  {row}")


def _save_files(key: str, items: list[dict], prefix: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    base = os.path.join(output_dir, f"{prefix}_{key}")

    with open(f"{base}.json", "w") as f:
        json.dump(items, f, indent=2, default=str)

    if items:
        with open(f"{base}.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(items[0].keys()), extrasaction="ignore")
            w.writeheader()
            w.writerows(items)
    else:
        open(f"{base}.csv", "w").close()


def _print_summary(
    name: str,
    counts: dict[str, int],
    permission_denied: dict[str, list[str]],
    other_errors: dict[str, list[str]],
) -> None:
    print(f"\n{'═' * 72}", file=sys.stderr)
    print(f"  Summary — {name}", file=sys.stderr)
    print(f"{'═' * 72}", file=sys.stderr)
    for key, label, _ in SECTIONS:
        if key not in counts:
            continue
        count = counts[key]
        perm  = permission_denied.get(key, [])
        errs  = other_errors.get(key, [])
        flags = ""
        if perm:
            flags += "  ⚠ permission denied on some endpoints"
        if errs:
            flags += f"  ✗ {len(errs)} error(s)"
        print(f"  {label:<38} {count:>5} found{flags}", file=sys.stderr)
    print("─" * 72, file=sys.stderr)

    all_perm = [ep for eps in permission_denied.values() for ep in eps]
    if all_perm:
        print(
            f"\n  Permission denied ({len(all_perm)} endpoints) — "
            f"results may be partial for this token.",
            file=sys.stderr,
        )
        for ep in all_perm:
            print(f"    • {ep}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Core run function
# ---------------------------------------------------------------------------


def run_workspace(
    name: str,
    w: WorkspaceClient,
    selected_sections: list,
    output_dir: str,
    save: bool,
    print_json: bool,
) -> dict[str, list[dict]]:
    print(f"\n{'━' * 72}", file=sys.stderr)
    print(f"  Workspace : {name}", file=sys.stderr)
    print(f"  Host      : {w.config.host}", file=sys.stderr)
    print(f"{'━' * 72}", file=sys.stderr)

    collector = InventoryCollector(w)
    ws_output_dir = os.path.join(output_dir, name)

    inventory: dict[str, list[dict]] = {}
    counts: dict[str, int] = {}
    permission_denied: dict[str, list[str]] = {}
    other_errors: dict[str, list[str]] = {}

    for key, label, fn in selected_sections:
        print(f"  Collecting {label}...", file=sys.stderr)
        items = fn(collector)
        perm, errs = collector.pop_errors()

        inventory[key] = items
        counts[key]    = len(items)
        if perm:
            permission_denied[key] = perm
        if errs:
            other_errors[key] = errs

        status = f"{len(items)} found"
        if perm:
            status += "  [partial — permission denied on some endpoints]"
        print(f"    -> {status}", file=sys.stderr)

        if save:
            _save_files(key, items, name, ws_output_dir)
            print(f"    -> saved to {ws_output_dir}/", file=sys.stderr)

    _print_summary(name, counts, permission_denied, other_errors)
    return inventory


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Corteva MIC — Databricks workspace inventory (v2, SDK-based)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config",     help="Path to workspaces JSON config (runs all workspaces)")
    parser.add_argument("--host",       default=os.environ.get("DATABRICKS_HOST", ""),  help="Single workspace host URL")
    parser.add_argument("--token",      default=os.environ.get("DATABRICKS_TOKEN", ""), help="Single workspace PAT token")
    parser.add_argument("--profile",    default="", help="~/.databrickscfg profile name")
    parser.add_argument("--save",       action="store_true", help="Write JSON + CSV files to disk")
    parser.add_argument("--output-dir", default="output",   help="Root output directory (default: ./output)")
    parser.add_argument("--json",       action="store_true", help="Print JSON to stdout (single workspace only)")
    parser.add_argument(
        "--section", choices=SECTION_KEYS, metavar="SECTION",
        help=f"Collect one section only. Choices: {', '.join(SECTION_KEYS)}",
    )
    args = parser.parse_args()

    selected = [
        (k, label, fn) for k, label, fn in SECTIONS
        if not args.section or k == args.section
    ]

    # ── Multi-workspace mode ──────────────────────────────────────────────────
    if args.config:
        with open(args.config) as f:
            workspaces = json.load(f)

        skipped = []
        for ws in workspaces:
            name    = ws.get("name", "unknown")
            host    = ws.get("host", "").strip()
            token   = ws.get("token", "").strip()
            profile = ws.get("profile", "").strip()

            if not host:
                print(f"\n  [{name}] Skipped — host not set in config.", file=sys.stderr)
                skipped.append(name)
                continue

            if not token and not profile:
                print(
                    f"\n  [{name}] Skipped — neither 'token' nor 'profile' set in config.",
                    file=sys.stderr,
                )
                skipped.append(name)
                continue

            try:
                w = _make_client(host=host, token=token, profile=profile)
                run_workspace(name, w, selected, args.output_dir, save=True, print_json=False)
            except Exception as exc:
                print(f"\n  [{name}] Failed to connect: {exc}", file=sys.stderr)
                skipped.append(name)

        if skipped:
            print(f"\n  Skipped workspaces: {', '.join(skipped)}", file=sys.stderr)
        return

    # ── Single workspace mode ─────────────────────────────────────────────────
    if not args.host and not args.profile:
        parser.error(
            "Provide --config for multi-workspace mode, "
            "or one of: --host + --token, --profile, "
            "or set DATABRICKS_HOST / DATABRICKS_TOKEN env vars."
        )

    w = _make_client(host=args.host, token=args.token, profile=args.profile)
    name = (
        args.host.split("//")[-1].split(".")[0]
        if args.host
        else (args.profile or "workspace")
    )

    inventory = run_workspace(
        name, w, selected,
        args.output_dir, save=args.save, print_json=args.json,
    )

    if args.json:
        print(json.dumps(inventory, indent=2, default=str))
    elif not args.save:
        for key, label, _ in selected:
            _print_section(label, inventory[key])
        print(f"\nTotal: {sum(len(v) for v in inventory.values())} resources")


if __name__ == "__main__":
    main()
