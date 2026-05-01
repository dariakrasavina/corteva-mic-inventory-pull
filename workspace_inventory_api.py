#!/usr/bin/env python3
"""
Corteva MIC — Workspace Inventory  (urllib version)
====================================================
No external dependencies — uses only Python built-ins (urllib, csv, json).
Does not require pip install. Works on any machine with Python 3.9+.

Authentication: PAT token only (--token dapi...).

Collects: jobs, pipelines, notebooks, tables, volumes, functions,
          genie spaces, ml experiments, dashboards, serving endpoints,
          apps, repos, registered ml models.

─── Single workspace ────────────────────────────────────────────────────────
    python workspace_inventory_api.py --host https://adb-xxx.azuredatabricks.net --token dapiXXX
    python workspace_inventory_api.py --host ... --token ... --save
    python workspace_inventory_api.py --host ... --token ... --section jobs
    python workspace_inventory_api.py --host ... --token ... --json > out.json

─── Multiple workspaces (reads workspaces.json) ─────────────────────────────
    python workspace_inventory_api.py --config workspaces.json
    python workspace_inventory_api.py --config workspaces.json --section tables

─── Options ─────────────────────────────────────────────────────────────────
    --config FILE       Path to workspaces JSON config (runs all workspaces)
    --host URL          Single workspace host URL
    --token TOKEN       Single workspace PAT token
    --save              Write JSON + CSV files to disk (always on with --config)
    --output-dir DIR    Root output directory (default: ~/corteva-mic-workspace-assets/output)
    --section KEY       Collect one section only
    --json              Print JSON to stdout (single workspace only)

─── workspaces.json format ──────────────────────────────────────────────────
    [
      { "name": "my-workspace-dev", "host": "https://adb-xxx.net", "token": "dapi..." },
      { "name": "my-workspace-prod", "host": "https://adb-yyy.net", "token": "dapi..." }
    ]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any


# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------


class DatabricksClient:
    def __init__(self, host: str, token: str) -> None:
        self.host = host.rstrip("/")
        self.token = token
        self._permission_errors: list[str] = []
        self._other_errors: list[str] = []

    def get(self, path: str, params: dict | None = None) -> Any:
        url = self.host + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url, headers={"Authorization": f"Bearer {self.token}"}
        )
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 403:
                self._permission_errors.append(path)
            elif e.code == 429:
                _warn(f"Rate limited on {path} — some results may be incomplete")
            else:
                self._other_errors.append(f"HTTP {e.code} {path}")
                _warn(f"HTTP {e.code} {path}: {e.read().decode()[:200]}")
            return {}
        except Exception as exc:  # noqa: BLE001
            self._other_errors.append(f"{path}: {exc}")
            _warn(f"{path}: {exc}")
            return {}

    def paginate(self, path: str, result_key: str, params: dict | None = None) -> list:
        results = []
        p = dict(params or {})
        while True:
            data = self.get(path, p)
            results.extend(data.get(result_key) or [])
            token = data.get("next_page_token")
            if not token:
                break
            p["page_token"] = token
        return results

    def uc_iter_schemas(self) -> list[tuple[str, str]]:
        """Return all (catalog, schema) pairs visible to this token."""
        pairs: list[tuple[str, str]] = []
        for cat in self.get("/api/2.1/unity-catalog/catalogs").get("catalogs", []):
            cat_name = cat["name"]
            for schema in self.get(
                "/api/2.1/unity-catalog/schemas", {"catalog_name": cat_name}
            ).get("schemas", []):
                pairs.append((cat_name, schema["name"]))
        return pairs

    def pop_errors(self) -> tuple[list[str], list[str]]:
        """Return and clear accumulated errors since the last call."""
        perm  = list(self._permission_errors)
        other = list(self._other_errors)
        self._permission_errors.clear()
        self._other_errors.clear()
        return perm, other


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _warn(msg: str) -> None:
    print(f"    [WARN] {msg}", file=sys.stderr)


def _fmt_ts(ms: int | None) -> str | None:
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _fmt_iso(s: str | None) -> str | None:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M UTC")
    except Exception:  # noqa: BLE001
        return s


# ---------------------------------------------------------------------------
# Collectors
# ---------------------------------------------------------------------------


def collect_jobs(c: DatabricksClient) -> list[dict]:
    raw = c.paginate("/api/2.1/jobs/list", "jobs", {"expand_tasks": "false", "limit": "100"})
    jobs = []
    for j in raw:
        jid      = j.get("job_id")
        settings = c.get("/api/2.1/jobs/get", {"job_id": jid}).get("settings", {})
        schedule = settings.get("schedule")
        trigger  = settings.get("trigger")
        cron     = schedule.get("quartz_cron_expression") if schedule else None
        tz_id    = schedule.get("timezone_id") if schedule else None
        paused   = schedule.get("pause_status") if schedule else None

        runs = c.get(
            "/api/2.1/jobs/runs/list",
            {"job_id": jid, "limit": "5", "expand_tasks": "false"},
        ).get("runs", [])
        last_run_time  = _fmt_ts(runs[0].get("start_time")) if runs else None
        last_run_state = (
            (runs[0].get("state") or {}).get("result_state")
            or (runs[0].get("state") or {}).get("life_cycle_state")
        ) if runs else None

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
            "name":            j.get("settings", {}).get("name"),
            "creator":         j.get("creator_user_name"),
            "status":          status,
            "is_scheduled":    bool(schedule),
            "schedule_cron":   cron,
            "schedule_tz":     tz_id,
            "schedule_paused": paused,
            "last_run_time":   last_run_time,
            "last_run_state":  last_run_state,
        })
    return jobs


def collect_pipelines(c: DatabricksClient) -> list[dict]:
    raw = c.get("/api/2.0/pipelines", {"max_results": "100"}).get("statuses", [])
    pipelines = []
    for p in raw:
        pid        = p.get("pipeline_id")
        detail     = c.get(f"/api/2.0/pipelines/{pid}")
        continuous = detail.get("continuous", False)
        trigger    = detail.get("trigger")
        last_mod   = _fmt_ts(detail.get("last_modified"))

        events = c.get(
            f"/api/2.0/pipelines/{pid}/events",
            {"max_results": "5", "order_by": "timestamp desc"},
        ).get("events", [])
        last_event_time = _fmt_iso(events[0].get("timestamp")) if events else None
        last_event_type = events[0].get("event_type") if events else None

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
            "name":            p.get("name"),
            "state":           p.get("state"),
            "creator":         p.get("creator_user_name"),
            "status":          status,
            "is_continuous":   continuous,
            "is_triggered":    bool(trigger),
            "last_modified":   last_mod,
            "last_run_time":   last_event_time,
            "last_event_type": last_event_type,
        })
    return pipelines


def collect_notebooks(c: DatabricksClient) -> list[dict]:
    notebooks: list[dict] = []

    def recurse(path: str) -> None:
        for obj in c.get("/api/2.0/workspace/list", {"path": path}).get("objects", []):
            if obj.get("object_type") == "NOTEBOOK":
                notebooks.append({
                    "object_id": obj.get("object_id"),
                    "path":      obj.get("path"),
                    "language":  obj.get("language"),
                })
            elif obj.get("object_type") == "DIRECTORY" and obj.get("path"):
                recurse(obj["path"])

    recurse("/")
    return notebooks


def collect_tables(c: DatabricksClient) -> list[dict]:
    tables: list[dict] = []
    for cat, schema in c.uc_iter_schemas():
        for t in c.paginate(
            "/api/2.1/unity-catalog/tables",
            "tables",
            {"catalog_name": cat, "schema_name": schema, "max_results": "200"},
        ):
            tables.append({
                "catalog_name":       t.get("catalog_name"),
                "schema_name":        t.get("schema_name"),
                "table_name":         t.get("name"),
                "full_name":          t.get("full_name"),
                "table_type":         t.get("table_type"),
                "data_source_format": t.get("data_source_format"),
                "storage_location":   t.get("storage_location"),
                "owner":              t.get("owner"),
                "created_at":         _fmt_ts(t.get("created_at")),
                "created_by":         t.get("created_by"),
                "updated_at":         _fmt_ts(t.get("updated_at")),
                "updated_by":         t.get("updated_by"),
                "comment":            t.get("comment"),
            })
    return tables


def collect_volumes(c: DatabricksClient) -> list[dict]:
    volumes: list[dict] = []
    for cat, schema in c.uc_iter_schemas():
        for v in c.paginate(
            "/api/2.1/unity-catalog/volumes",
            "volumes",
            {"catalog_name": cat, "schema_name": schema, "max_results": "200"},
        ):
            volumes.append({
                "catalog_name":     v.get("catalog_name"),
                "schema_name":      v.get("schema_name"),
                "volume_name":      v.get("name"),
                "full_name":        v.get("full_name"),
                "volume_type":      v.get("volume_type"),
                "storage_location": v.get("storage_location"),
                "owner":            v.get("owner"),
                "created_at":       _fmt_ts(v.get("created_at")),
                "created_by":       v.get("created_by"),
                "updated_at":       _fmt_ts(v.get("updated_at")),
                "updated_by":       v.get("updated_by"),
                "comment":          v.get("comment"),
            })
    return volumes


def collect_functions(c: DatabricksClient) -> list[dict]:
    functions: list[dict] = []
    for cat, schema in c.uc_iter_schemas():
        for fn in c.paginate(
            "/api/2.1/unity-catalog/functions",
            "functions",
            {"catalog_name": cat, "schema_name": schema, "max_results": "200"},
        ):
            functions.append({
                "catalog_name":       fn.get("catalog_name"),
                "schema_name":        fn.get("schema_name"),
                "function_name":      fn.get("name"),
                "full_name":          fn.get("full_name"),
                "data_type":          fn.get("data_type"),
                "routine_body":       fn.get("routine_body"),
                "routine_definition": fn.get("routine_definition"),
                "language":           fn.get("external_language") or fn.get("routine_body"),
                "owner":              fn.get("owner"),
                "created_at":         _fmt_ts(fn.get("created_at")),
                "created_by":         fn.get("created_by"),
                "updated_at":         _fmt_ts(fn.get("updated_at")),
                "updated_by":         fn.get("updated_by"),
                "comment":            fn.get("comment"),
            })
    return functions


def collect_genie_spaces(c: DatabricksClient) -> list[dict]:
    return [
        {
            "space_id": s.get("id") or s.get("space_id"),
            "title":    s.get("title") or s.get("name"),
        }
        for s in c.get("/api/2.0/genie/spaces").get("spaces", [])
    ]


def collect_experiments(c: DatabricksClient) -> list[dict]:
    exps = c.paginate(
        "/api/2.0/mlflow/experiments/search", "experiments", {"max_results": "1000"}
    )
    if not exps:
        exps = c.get("/api/2.0/mlflow/experiments/list").get("experiments", [])
    return [
        {
            "experiment_id":     e.get("experiment_id"),
            "name":              e.get("name"),
            "lifecycle_stage":   e.get("lifecycle_stage"),
            "artifact_location": e.get("artifact_location"),
        }
        for e in exps
    ]


def collect_dashboards(c: DatabricksClient) -> list[dict]:
    items: list[dict] = []
    for d in c.paginate("/api/2.0/lakeview/dashboards", "dashboards", {"page_size": "100"}):
        items.append({
            "dashboard_id": d.get("dashboard_id"),
            "display_name": d.get("display_name"),
            "path":         d.get("path"),
            "type":         "lakeview",
        })
    for d in c.get("/api/2.0/preview/sql/dashboards", {"page_size": "250"}).get("results", []):
        items.append({
            "dashboard_id": d.get("id"),
            "display_name": d.get("name"),
            "type":         "classic",
        })
    return items


def collect_serving_endpoints(c: DatabricksClient) -> list[dict]:
    return [
        {
            "name":               e.get("name"),
            "creator":            e.get("creator"),
            "state_ready":        (e.get("state") or {}).get("ready"),
            "state_config_update":(e.get("state") or {}).get("config_update"),
        }
        for e in c.get("/api/2.0/serving-endpoints").get("endpoints", [])
    ]


def collect_apps(c: DatabricksClient) -> list[dict]:
    return [
        {
            "name":        a.get("name"),
            "description": a.get("description"),
            "status":      (a.get("status") or {}).get("state"),
            "url":         a.get("url"),
        }
        for a in c.paginate("/api/2.0/apps", "apps", {"page_size": "100"})
    ]


def collect_repos(c: DatabricksClient) -> list[dict]:
    """Git folders / Repos — scans /Users and /Repos workspace paths."""

    def find_repos(path: str) -> list[dict]:
        found: list[dict] = []
        for obj in c.get("/api/2.0/workspace/list", {"path": path}).get("objects", []):
            if obj.get("object_type") == "REPO":
                found.append(obj)
            elif obj.get("object_type") == "DIRECTORY" and obj.get("path"):
                found.extend(find_repos(obj["path"]))
        return found

    raw: list[dict] = []
    for root in ("/Users", "/Repos"):
        raw.extend(find_repos(root))

    repos: list[dict] = []
    for r in raw:
        rid    = r.get("resource_id") or r.get("object_id")
        detail = c.get(f"/api/2.0/repos/{rid}")
        path   = detail.get("path") or r.get("path", "")
        owner  = path.split("/")[2] if path.count("/") >= 2 else None
        repos.append({
            "repo_id":        detail.get("id") or rid,
            "path":           path,
            "url":            detail.get("url"),
            "provider":       detail.get("provider"),
            "branch":         detail.get("branch"),
            "head_commit_id": detail.get("head_commit_id"),
            "owner":          owner,
        })
    return repos


def collect_registered_models(c: DatabricksClient) -> list[dict]:
    models: list[dict] = []
    for m in c.paginate(
        "/api/2.1/unity-catalog/models", "registered_models", {"max_results": "200"}
    ):
        models.append({
            "full_name":    m.get("full_name"),
            "catalog_name": m.get("catalog_name"),
            "schema_name":  m.get("schema_name"),
            "name":         m.get("name"),
            "owner":        m.get("owner"),
            "type":         "unity_catalog",
        })
    for m in c.paginate(
        "/api/2.0/mlflow/registered-models/list", "registered_models", {"max_results": "1000"}
    ):
        models.append({"name": m.get("name"), "type": "workspace_registry"})
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

    with open(f"{base}.json", "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, default=str, ensure_ascii=False)

    if items:
        with open(f"{base}.csv", "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=list(items[0].keys()), extrasaction="ignore")
            w.writeheader()
            w.writerows(items)
    else:
        open(f"{base}.csv", "w", encoding="utf-8-sig").close()


def _print_summary(
    name: str,
    counts: dict[str, int],
    permission_denied: dict[str, list[str]],
    other_errors: dict[str, list[str]],
) -> None:
    sep = "─" * 72
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
    print(sep, file=sys.stderr)

    all_perm = [ep for eps in permission_denied.values() for ep in eps]
    if all_perm:
        print(f"\n  Permission denied ({len(all_perm)} endpoints) —"
              f" results may be partial for this token.", file=sys.stderr)
        for ep in all_perm:
            print(f"    • {ep}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Core run function
# ---------------------------------------------------------------------------


def run_workspace(
    name: str,
    host: str,
    token: str,
    selected_sections: list,
    output_dir: str,
    save: bool,
    print_json: bool,
) -> dict[str, list[dict]]:
    print(f"\n{'━' * 72}", file=sys.stderr)
    print(f"  Workspace : {name}", file=sys.stderr)
    print(f"  Host      : {host}", file=sys.stderr)
    print(f"{'━' * 72}", file=sys.stderr)

    client = DatabricksClient(host, token)
    ws_output_dir = os.path.join(output_dir, name)

    inventory: dict[str, list[dict]] = {}
    counts: dict[str, int] = {}
    permission_denied: dict[str, list[str]] = {}
    other_errors: dict[str, list[str]] = {}

    for key, label, fn in selected_sections:
        print(f"  Collecting {label}...", file=sys.stderr)
        items = fn(client)
        perm, errs = client.pop_errors()

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
            print(f"    -> saved to output/{name}/", file=sys.stderr)

    _print_summary(name, counts, permission_denied, other_errors)
    return inventory


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Corteva MIC — Databricks workspace inventory",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config",     help="Path to workspaces JSON config (runs all workspaces)")
    parser.add_argument("--host",       default=os.environ.get("DATABRICKS_HOST", ""), help="Single workspace host URL")
    parser.add_argument("--token",      default=os.environ.get("DATABRICKS_TOKEN", ""), help="Single workspace PAT token")
    parser.add_argument("--save",       action="store_true", help="Write JSON + CSV files to disk")
    parser.add_argument("--output-dir", default=os.path.expanduser("~/corteva-mic-workspace-assets/output"), help="Root output directory (default: ~/corteva-mic-workspace-assets/output)")
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
            name  = ws.get("name", "unknown")
            host  = ws.get("host", "").strip()
            token = ws.get("token", "").strip()

            if not host or not token:
                print(f"\n  [{name}] Skipped — host or token not set in config.", file=sys.stderr)
                skipped.append(name)
                continue

            run_workspace(name, host, token, selected, args.output_dir, save=True, print_json=False)

        if skipped:
            print(f"\n  Skipped workspaces (missing config): {', '.join(skipped)}", file=sys.stderr)
        return

    # ── Single workspace mode ─────────────────────────────────────────────────
    if not args.host or not args.token:
        parser.error(
            "Provide --config for multi-workspace mode, "
            "or --host + --token for single workspace mode "
            "(env vars DATABRICKS_HOST / DATABRICKS_TOKEN also accepted)."
        )

    name = args.host.split("//")[-1].split(".")[0]
    inventory = run_workspace(
        name, args.host, args.token, selected,
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
