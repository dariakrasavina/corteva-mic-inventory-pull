#!/usr/bin/env python3
"""
Corteva MIC — Workspace Inventory  (SDK version)
=================================================
Uses the official Databricks Python SDK for authentication and API calls.
Supports PAT tokens, ~/.databrickscfg profiles, OAuth M2M (service principals),
and environment variables. Recommended for production and automated runs.

Install SDK:  pip3 install databricks-sdk --index-url https://pypi-proxy.dev.databricks.com/simple

─── Authentication options ──────────────────────────────────────────────────
    PAT token:         --host URL --token dapi...
    CLI profile:       --profile my-profile   (reads ~/.databrickscfg)
    Env vars:          DATABRICKS_HOST + DATABRICKS_TOKEN
    Service principal: DATABRICKS_HOST + DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET

─── Single workspace ────────────────────────────────────────────────────────
    python workspace_inventory_sdk.py --host https://adb-xxx.net --token dapi...
    python workspace_inventory_sdk.py --profile my-profile
    python workspace_inventory_sdk.py --profile my-profile --section jobs

─── Multiple workspaces (reads workspaces.json) ─────────────────────────────
    python workspace_inventory_sdk.py --config workspaces.json

─── Options ─────────────────────────────────────────────────────────────────
    --config FILE       Path to workspaces JSON config (runs all workspaces)
    --host URL          Single workspace host URL
    --token TOKEN       Single workspace PAT token
    --profile NAME      ~/.databrickscfg profile name
    --save              Write JSON + CSV files to disk (always on with --config)
    --output-dir DIR    Root output directory (default: ~/corteva-mic-workspace-assets/output)
    --section KEY       Collect one section only
    --json              Print JSON to stdout (single workspace only)

─── Volume-walk settings (used by the `volume_files` section) ────────────────
    EXTERNAL volumes are ALWAYS excluded — both the `volumes` (metadata) and
    `volume_files` (recursive) collectors only emit MANAGED volumes. This
    mirrors volume_artifacts_inventory.py.

    --volume-files-catalog NAME   Catalog to walk for `volume_files`
                                  (default: mic_prod). Pass '' to walk all
                                  catalogs the token can see.
    --list-timeout SEC            Per-directory listing timeout (default 60s).
                                  Wraps list(iterator) materialization, not
                                  the bare SDK call.
    --volume-timeout SEC          Cumulative budget per volume (default 600s).
                                  Once spent, the volume is abandoned and
                                  the next one starts.

─── workspaces.json format — supports token or profile per workspace ─────────
    [
      {"name": "dev",  "host": "https://adb-xxx.net", "token": "dapi..."},
      {"name": "uat",  "host": "https://adb-yyy.net", "profile": "uat-profile"},
      {"name": "prod", "host": "https://adb-zzz.net", "profile": "prod-profile"}
    ]
"""

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist


# Defaults for the volume_files collector — match volume_artifacts_inventory.py
DEFAULT_VOLUME_FILES_CATALOG = "mic_prod"
DEFAULT_LIST_TIMEOUT_SEC = 60
DEFAULT_VOLUME_TIMEOUT_SEC = 600


class _VolumeBudgetExceeded(Exception):
    """Raised inside the volume_files walk when per-volume time budget is spent."""


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
    def __init__(
        self,
        w: WorkspaceClient,
        volume_files_catalog: str = DEFAULT_VOLUME_FILES_CATALOG,
        list_timeout: float = DEFAULT_LIST_TIMEOUT_SEC,
        volume_timeout: float = DEFAULT_VOLUME_TIMEOUT_SEC,
    ) -> None:
        self.w = w
        # Volume-walk config (used by collect_volume_files; EXTERNAL volumes
        # are always excluded — no flag).
        self.volume_files_catalog = volume_files_catalog
        self.list_timeout = list_timeout
        self.volume_timeout = volume_timeout
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


def _list_dir_with_timeout(w: WorkspaceClient, path: str, timeout: float) -> list | None:
    """Materialize files.list_directory_contents with a wall-clock timeout.

    Wrapping `list(iterator)` (not the bare SDK call) is critical: the SDK
    method returns a paginating generator, so the actual API work happens
    during iteration. A timeout on the bare call would fire on a no-op.

    Returns the entries list on success, None on timeout, [] on other errors.
    """
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        return pool.submit(
            lambda: list(w.files.list_directory_contents(directory_path=path))
        ).result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        return None
    except PermissionDenied:
        _warn(f"permission denied: {path}")
        return []
    except (NotFound, ResourceDoesNotExist):
        return []
    except Exception as exc:  # noqa: BLE001
        _warn(f"files.list_directory_contents({path}): {exc}")
        return []
    finally:
        pool.shutdown(wait=False)


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

        tags = dict(settings.tags) if settings and settings.tags else {}
        dab_bundle = tags.get("bundle.name", "")
        dab_target = tags.get("bundle.target", "")

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
            "dab_managed":     "yes" if dab_bundle else "no",
            "dab_bundle":      dab_bundle,
            "dab_target":      dab_target,
        })
    return jobs


def collect_pipelines(c: InventoryCollector) -> list[dict]:
    raw = c.safe("pipelines.list", lambda: list(c.w.pipelines.list_pipelines()), default=[])
    pipelines = []
    for p in raw:
        pid = p.pipeline_id

        _pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        try:
            detail = _pool.submit(lambda pid=pid: c.w.pipelines.get(pipeline_id=pid)).result(timeout=10)
        except concurrent.futures.TimeoutError:
            _warn(f"pipelines.get({pid}): timed out — skipping detail for this pipeline")
            detail = None
        except Exception as exc:
            _warn(f"pipelines.get({pid}): {exc}")
            detail = None
        finally:
            _pool.shutdown(wait=False)

        spec = getattr(detail, "spec", None) if detail else None
        spec = spec or detail  # older SDK versions put fields directly on the response
        continuous = getattr(spec, "continuous", False) or False
        trigger = getattr(spec, "trigger", None)
        config = dict(getattr(spec, "configuration", None) or {})
        dab_source = config.get("bundle.sourcePath", "")

        updates = getattr(p, "latest_updates", None) or []
        last_update = updates[0] if updates else None
        last_run_time = _fmt_dt(getattr(last_update, "creation_time", None)) if last_update else None
        last_run_state = _val(getattr(last_update, "state", None)) if last_update else None

        if continuous:
            status = "CONTINUOUS"
        elif trigger:
            status = "TRIGGERED"
        elif last_run_time:
            status = "MANUAL (has runs)"
        else:
            status = _val(p.state) or "INACTIVE"

        pipelines.append({
            "pipeline_id":     pid,
            "name":            p.name,
            "state":           _val(p.state),
            "creator":         p.creator_user_name,
            "status":          status,
            "is_continuous":   bool(continuous),
            "is_triggered":    bool(trigger),
            "last_run_time":   last_run_time,
            "last_run_state":  last_run_state,
            "dab_managed":     "yes" if dab_source else "no",
            "dab_source_path": dab_source,
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
    """List MANAGED UC volume metadata across all catalogs the token can see.

    EXTERNAL volumes are always excluded — same scoping as the volume_files
    collector and the standalone volume_artifacts_inventory.py script.
    """
    volumes: list[dict] = []
    skipped_external = 0
    for cat, schema in c._uc_iter_schemas():
        rows = c.safe(
            f"volumes.list({cat}.{schema})",
            lambda cn=cat, sn=schema: list(c.w.volumes.list(catalog_name=cn, schema_name=sn)),
            default=[],
        )
        for v in rows:
            if _val(v.volume_type) != "MANAGED":
                skipped_external += 1
                continue
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
    if skipped_external:
        _warn(f"excluded {skipped_external} EXTERNAL volume(s)")
    return volumes


def collect_volume_files(c: InventoryCollector) -> list[dict]:
    """Recursively list every file inside MANAGED UC volumes.

    Mirrors volume_artifacts_inventory.py:
      - EXTERNAL volumes are always excluded (raw data-lake mounts that
        don't hold ML artifacts and can have millions of files).
      - Catalog scope defaults to `c.volume_files_catalog` (mic_prod).
        Pass an empty string via --volume-files-catalog '' to walk all.
      - Per-listing timeout (c.list_timeout, default 60s) wraps the
        list(iterator) materialization, since the SDK call returns a
        paginating generator and a timeout on the bare call is a no-op.
      - Per-volume cumulative budget (c.volume_timeout, default 600s) —
        once spent, the volume is abandoned and the next one starts.
      - Each directory is logged before listing so a hang is visible.
    """
    files: list[dict] = []
    skipped_external = 0

    def walk(v: Any, path: str, deadline: float) -> None:
        if time.monotonic() > deadline:
            raise _VolumeBudgetExceeded(path)
        print(f"    listing {path}", file=sys.stderr)
        entries = _list_dir_with_timeout(c.w, path, c.list_timeout)
        if entries is None:
            print(
                f"    [TIMEOUT] {path} (>{c.list_timeout:.0f}s) — skipped",
                file=sys.stderr,
            )
            return
        for entry in entries:
            if time.monotonic() > deadline:
                raise _VolumeBudgetExceeded(path)
            entry_path = entry.path or ""
            entry_name = entry.name or os.path.basename(entry_path)
            if getattr(entry, "is_directory", False):
                if entry_path:
                    walk(v, entry_path, deadline)
                continue
            mod_ms = getattr(entry, "last_modified", None) or getattr(entry, "modification_time", None)
            ext = os.path.splitext(entry_name)[1].lstrip(".").lower()
            files.append({
                "catalog_name":      v.catalog_name,
                "schema_name":       v.schema_name,
                "volume_name":       v.name,
                "full_volume_name":  v.full_name,
                "volume_type":       _val(v.volume_type),
                "file_path":         entry_path,
                "file_name":         entry_name,
                "file_extension":    ext,
                "file_size_bytes":   getattr(entry, "file_size", None),
                "modification_time": _fmt_ts(mod_ms),
            })

    # Resolve catalog scope: explicit single catalog or all-visible if blank.
    if c.volume_files_catalog:
        catalogs = [c.volume_files_catalog]
    else:
        cats = c.safe("catalogs.list", lambda: list(c.w.catalogs.list()), default=[])
        catalogs = [cat.name for cat in cats]

    for cat in catalogs:
        schemas = c.safe(
            f"schemas.list({cat})",
            lambda cn=cat: list(c.w.schemas.list(catalog_name=cn)),
            default=[],
        )
        for sch in schemas:
            sn = sch.name
            vols = c.safe(
                f"volumes.list({cat}.{sn})",
                lambda cn=cat, ssn=sn: list(c.w.volumes.list(catalog_name=cn, schema_name=ssn)),
                default=[],
            )
            for v in vols:
                if _val(v.volume_type) != "MANAGED":
                    skipped_external += 1
                    continue
                root = f"/Volumes/{v.catalog_name}/{v.schema_name}/{v.name}"
                print(f"  ━━ {root}", file=sys.stderr)
                deadline = time.monotonic() + c.volume_timeout
                before = len(files)
                try:
                    walk(v, root, deadline)
                except _VolumeBudgetExceeded as exc:
                    print(
                        f"  [BUDGET] {root} — exceeded {c.volume_timeout:.0f}s, "
                        f"abandoning at {exc} (captured {len(files) - before} files before bailout)",
                        file=sys.stderr,
                    )

    if skipped_external:
        _warn(f"excluded {skipped_external} EXTERNAL volume(s)")
    return files


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
            "status":      _val(getattr(getattr(a, "status", None), "state", None)),
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

    # Legacy workspace model registry — try SDK first, fall back to direct API
    legacy = c.safe(
        "model_registry.list (legacy)",
        lambda: list(c.w.model_registry.list_registered_models(max_results=1000)),
        default=None,
    )
    if legacy is None:
        # SDK method unavailable in this version — fall back to direct HTTP call
        try:
            data = c.w.api_client.do(
                "GET", "/api/2.0/mlflow/registered-models/list", query={"max_results": 1000}
            )
            legacy = data.get("registered_models") or []
            for m in legacy:
                models.append({
                    "full_name":    None,
                    "catalog_name": None,
                    "schema_name":  None,
                    "name":         m.get("name"),
                    "owner":        None,
                    "type":         "workspace_registry",
                })
        except Exception as exc:  # noqa: BLE001
            c._other_errors.append(f"model_registry.list (legacy fallback): {exc}")
    else:
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


def collect_model_versions(c: InventoryCollector) -> list[dict]:
    """List every version of every registered model — UC and legacy workspace registry.

    UC versions carry aliases (Production/Champion/etc.); legacy versions carry stages.
    """
    versions: list[dict] = []

    # ── UC model versions ────────────────────────────────────────────────────
    uc_models = c.safe(
        "registered_models.list (UC)",
        lambda: list(c.w.registered_models.list()),
        default=[],
    )
    for m in uc_models:
        full_name = m.full_name
        rows = c.safe(
            f"model_versions.list({full_name})",
            lambda fn=full_name: list(c.w.model_versions.list(full_name=fn)),
            default=[],
        )
        for v in rows:
            alias_names = [
                getattr(a, "alias_name", None) or getattr(a, "name", None)
                for a in (getattr(v, "aliases", None) or [])
            ]
            versions.append({
                "type":         "unity_catalog",
                "full_name":    full_name,
                "model_name":   m.name,
                "version":      getattr(v, "version", None),
                "aliases":      ", ".join(a for a in alias_names if a),
                "stage":        None,
                "status":       _val(getattr(v, "status", None)),
                "source":       getattr(v, "source", None),
                "run_id":       getattr(v, "run_id", None),
                "created_at":   _fmt_ts(getattr(v, "created_at", None)),
                "created_by":   getattr(v, "created_by", None),
                "updated_at":   _fmt_ts(getattr(v, "updated_at", None)),
                "comment":      getattr(v, "comment", None),
            })

    # ── Legacy workspace registry versions ───────────────────────────────────
    legacy = c.safe(
        "model_registry.list (legacy)",
        lambda: list(c.w.model_registry.list_registered_models(max_results=1000)),
        default=None,
    )

    if legacy is None:
        # Fallback to direct REST call
        try:
            data = c.w.api_client.do(
                "GET", "/api/2.0/mlflow/registered-models/list", query={"max_results": 1000}
            )
            legacy_names = [m.get("name") for m in (data.get("registered_models") or [])]
        except Exception as exc:  # noqa: BLE001
            c._other_errors.append(f"model_registry.list (legacy fallback): {exc}")
            legacy_names = []
    else:
        legacy_names = [m.name for m in legacy]

    for name in legacy_names:
        if not name:
            continue
        rows = c.safe(
            f"model_registry.search_model_versions({name})",
            lambda nm=name: list(
                c.w.model_registry.search_model_versions(filter=f"name='{nm}'")
            ),
            default=None,
        )
        if rows is None:
            # Fallback: REST search
            try:
                data = c.w.api_client.do(
                    "GET",
                    "/api/2.0/mlflow/model-versions/search",
                    query={"filter": f"name='{name}'", "max_results": 1000},
                )
                rows = data.get("model_versions") or []
                rows_iter = iter(rows)
                rows = [_LegacyVersion(r) for r in rows_iter]
            except Exception as exc:  # noqa: BLE001
                c._other_errors.append(f"model_versions search (legacy fallback): {exc}")
                rows = []

        for v in rows:
            versions.append({
                "type":         "workspace_registry",
                "full_name":    None,
                "model_name":   getattr(v, "name", name),
                "version":      getattr(v, "version", None),
                "aliases":      None,
                "stage":        _val(getattr(v, "current_stage", None)),
                "status":       _val(getattr(v, "status", None)),
                "source":       getattr(v, "source", None),
                "run_id":       getattr(v, "run_id", None),
                "created_at":   _fmt_ts(getattr(v, "creation_timestamp", None)),
                "created_by":   getattr(v, "user_id", None),
                "updated_at":   _fmt_ts(getattr(v, "last_updated_timestamp", None)),
                "comment":      getattr(v, "description", None),
            })

    return versions


class _LegacyVersion:
    """Lightweight wrapper so REST-fallback rows match the SDK attribute shape."""

    def __init__(self, raw: dict) -> None:
        self.name = raw.get("name")
        self.version = raw.get("version")
        self.current_stage = raw.get("current_stage")
        self.status = raw.get("status")
        self.source = raw.get("source")
        self.run_id = raw.get("run_id")
        self.creation_timestamp = raw.get("creation_timestamp")
        self.last_updated_timestamp = raw.get("last_updated_timestamp")
        self.user_id = raw.get("user_id")
        self.description = raw.get("description")


# ---------------------------------------------------------------------------
# Section registry
# ---------------------------------------------------------------------------

SECTIONS = [
    ("jobs",              "Jobs",                               collect_jobs),
    ("pipelines",         "DLT Pipelines",                      collect_pipelines),
    ("notebooks",         "Notebooks",                          collect_notebooks),
    ("tables",            "Tables (Unity Catalog)",             collect_tables),
    ("volumes",           "Volumes (Unity Catalog)",            collect_volumes),
    ("volume_files",      "Volume Files (UC, recursive)",       collect_volume_files),
    ("functions",         "Functions (Unity Catalog)",          collect_functions),
    ("genie_spaces",      "Genie Spaces",                       collect_genie_spaces),
    ("experiments",       "ML Experiments",                     collect_experiments),
    ("dashboards",        "Dashboards",                         collect_dashboards),
    ("serving_endpoints", "Serving Endpoints (agents/models)",  collect_serving_endpoints),
    ("apps",              "Apps",                               collect_apps),
    ("repos",             "Repos / Git Folders",                collect_repos),
    ("registered_models", "Registered ML Models",               collect_registered_models),
    ("model_versions",    "Model Versions (UC + legacy)",       collect_model_versions),
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
    volume_files_catalog: str = DEFAULT_VOLUME_FILES_CATALOG,
    list_timeout: float = DEFAULT_LIST_TIMEOUT_SEC,
    volume_timeout: float = DEFAULT_VOLUME_TIMEOUT_SEC,
) -> dict[str, list[dict]]:
    print(f"\n{'━' * 72}", file=sys.stderr)
    print(f"  Workspace : {name}", file=sys.stderr)
    print(f"  Host      : {w.config.host}", file=sys.stderr)
    print(
        f"  Volumes   : MANAGED only (EXTERNAL always skipped); "
        f"volume_files catalog = {volume_files_catalog or '(all)'}",
        file=sys.stderr,
    )
    print(f"{'━' * 72}", file=sys.stderr)

    collector = InventoryCollector(
        w,
        volume_files_catalog=volume_files_catalog,
        list_timeout=list_timeout,
        volume_timeout=volume_timeout,
    )
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
    parser.add_argument("--output-dir", default=os.path.expanduser("~/corteva-mic-workspace-assets/output"), help="Root output directory (default: ~/corteva-mic-workspace-assets/output)")
    parser.add_argument("--json",       action="store_true", help="Print JSON to stdout (single workspace only)")
    parser.add_argument(
        "--section", choices=SECTION_KEYS, metavar="SECTION",
        help=f"Collect one section only. Choices: {', '.join(SECTION_KEYS)}",
    )
    parser.add_argument(
        "--volume-files-catalog", default=DEFAULT_VOLUME_FILES_CATALOG,
        help=f"Catalog to walk for the `volume_files` section "
             f"(default: {DEFAULT_VOLUME_FILES_CATALOG}). Pass '' to walk every "
             f"catalog the token can see. EXTERNAL volumes are always excluded.",
    )
    parser.add_argument(
        "--list-timeout", type=float, default=DEFAULT_LIST_TIMEOUT_SEC,
        help=f"Per-directory listing timeout for the volume_files walk "
             f"(default: {DEFAULT_LIST_TIMEOUT_SEC}s). On timeout, that one "
             f"directory is skipped and the walk continues.",
    )
    parser.add_argument(
        "--volume-timeout", type=float, default=DEFAULT_VOLUME_TIMEOUT_SEC,
        help=f"Cumulative time budget per volume "
             f"(default: {DEFAULT_VOLUME_TIMEOUT_SEC}s). Once spent, the "
             f"volume is abandoned and the next one starts.",
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
                run_workspace(name, w, selected, args.output_dir,
                              save=True, print_json=False,
                              volume_files_catalog=args.volume_files_catalog,
                              list_timeout=args.list_timeout,
                              volume_timeout=args.volume_timeout)
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
        volume_files_catalog=args.volume_files_catalog,
        list_timeout=args.list_timeout,
        volume_timeout=args.volume_timeout,
    )

    if args.json:
        print(json.dumps(inventory, indent=2, default=str))
    elif not args.save:
        for key, label, _ in selected:
            _print_section(label, inventory[key])
        print(f"\nTotal: {sum(len(v) for v in inventory.values())} resources")


if __name__ == "__main__":
    main()
