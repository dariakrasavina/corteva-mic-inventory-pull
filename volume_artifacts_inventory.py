#!/usr/bin/env python3
"""
Corteva MIC — UC Volume Artifacts Inventory
============================================
Standalone script: lists every file inside one or more Unity Catalog volumes,
producing CSV+JSON of file paths, sizes, extensions, and modification times.

Built to hand to an ML engineer who needs to see what artifacts (.pkl,
.joblib, .parquet, .json, etc.) live inside specific Databricks volumes
without running the full workspace inventory.

Install:  pip install databricks-sdk

─── Auth ─────────────────────────────────────────────────────────────────────
    PAT token:    --host URL --token dapi...
    CLI profile:  --profile my-profile        (reads ~/.databrickscfg)
    Env vars:     DATABRICKS_HOST + DATABRICKS_TOKEN

─── Scope filters (combine any) ──────────────────────────────────────────────
    --volume CATALOG.SCHEMA.NAME    Single volume (fastest)
    --catalog NAME                  Limit to one catalog
    --schema NAME                   Limit to one schema (requires --catalog)
    --extension EXT                 Keep only files with this extension (e.g. pkl)
    --managed-only                  Skip EXTERNAL volumes (raw data-lake mounts).
                                    Recommended for ML artifact use cases.
    (no filters)                    Walks every volume the token can see

─── Examples ─────────────────────────────────────────────────────────────────
    # Just the .pkl artifacts in one volume
    python volume_artifacts_inventory.py --profile mic-prod \\
        --volume mic_prod.gold_seed_forecasting.artifacts \\
        --extension pkl --save

    # Every file in every volume of one schema
    python volume_artifacts_inventory.py --profile mic-prod \\
        --catalog mic_prod --schema gold_seed_forecasting --save

    # Multi-workspace via config
    python volume_artifacts_inventory.py --config workspaces.json \\
        --catalog mic_prod --extension pkl

─── workspaces.json format ───────────────────────────────────────────────────
    [
      {"name": "dev",  "host": "https://adb-xxx.net", "profile": "dev-profile"},
      {"name": "prod", "host": "https://adb-zzz.net", "token":   "dapi..."}
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


DEFAULT_OUTPUT_DIR = os.path.expanduser("~/corteva-mic-workspace-assets/output")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client(host: str = "", token: str = "", profile: str = "") -> WorkspaceClient:
    if profile:
        return WorkspaceClient(profile=profile)
    if host and token:
        return WorkspaceClient(host=host, token=token)
    if host:
        return WorkspaceClient(host=host)
    return WorkspaceClient()


def _fmt_ts(ms: int | float | None) -> str | None:
    if not ms:
        return None
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _safe(label: str, fn: Any, default: Any = None) -> Any:
    try:
        return fn()
    except PermissionDenied:
        print(f"    [WARN] permission denied: {label}", file=sys.stderr)
    except (NotFound, ResourceDoesNotExist):
        pass
    except Exception as exc:  # noqa: BLE001
        print(f"    [WARN] {label}: {exc}", file=sys.stderr)
    return default


# ---------------------------------------------------------------------------
# Volume discovery + recursive walk
# ---------------------------------------------------------------------------


def _volume_type(v: Any) -> str:
    vt = getattr(v, "volume_type", None)
    if vt is None:
        return ""
    return getattr(vt, "value", str(vt)).upper()


def _list_target_volumes(
    w: WorkspaceClient,
    catalog: str,
    schema: str,
    volume_full_name: str,
    managed_only: bool,
) -> list:
    """Resolve --volume / --catalog / --schema flags into the set of volumes to walk."""
    if volume_full_name:
        v = _safe(
            f"volumes.read({volume_full_name})",
            lambda: w.volumes.read(name=volume_full_name),
        )
        if not v:
            return []
        if managed_only and _volume_type(v) != "MANAGED":
            print(f"    [SKIP] {volume_full_name} is {_volume_type(v)} (managed-only mode)", file=sys.stderr)
            return []
        return [v]

    if catalog:
        catalogs = [catalog]
    else:
        cats = _safe("catalogs.list", lambda: list(w.catalogs.list()), default=[])
        catalogs = [c.name for c in cats]

    volumes: list = []
    for cat in catalogs:
        if schema:
            schemas = [schema]
        else:
            schs = _safe(
                f"schemas.list({cat})",
                lambda c=cat: list(w.schemas.list(catalog_name=c)),
                default=[],
            )
            schemas = [s.name for s in schs]
        for sch in schemas:
            rows = _safe(
                f"volumes.list({cat}.{sch})",
                lambda c=cat, s=sch: list(w.volumes.list(catalog_name=c, schema_name=s)),
                default=[],
            )
            volumes.extend(rows)

    if managed_only:
        before = len(volumes)
        volumes = [v for v in volumes if _volume_type(v) == "MANAGED"]
        skipped = before - len(volumes)
        if skipped:
            print(f"  [INFO] managed-only: skipping {skipped} EXTERNAL volume(s)", file=sys.stderr)
    return volumes


def _walk(
    w: WorkspaceClient,
    v: Any,
    path: str,
    ext_filter: str,
    sink: list[dict],
) -> None:
    entries = _safe(
        f"files.list_directory_contents({path})",
        lambda: list(w.files.list_directory_contents(directory_path=path)),
        default=[],
    )
    for e in entries:
        epath = e.path or ""
        ename = e.name or os.path.basename(epath)
        if getattr(e, "is_directory", False):
            if epath:
                _walk(w, v, epath, ext_filter, sink)
            continue
        ext = os.path.splitext(ename)[1].lstrip(".").lower()
        if ext_filter and ext != ext_filter:
            continue
        mod_ms = getattr(e, "last_modified", None) or getattr(e, "modification_time", None)
        sink.append({
            "catalog_name":      v.catalog_name,
            "schema_name":       v.schema_name,
            "volume_name":       v.name,
            "full_volume_name":  v.full_name,
            "file_path":         epath,
            "file_name":         ename,
            "file_extension":    ext,
            "file_size_bytes":   getattr(e, "file_size", None),
            "modification_time": _fmt_ts(mod_ms),
        })


def collect(
    w: WorkspaceClient,
    catalog: str,
    schema: str,
    volume_full_name: str,
    ext_filter: str,
    managed_only: bool,
) -> list[dict]:
    volumes = _list_target_volumes(w, catalog, schema, volume_full_name, managed_only)
    print(f"  Walking {len(volumes)} volume(s)...", file=sys.stderr)
    files: list[dict] = []
    for v in volumes:
        root = f"/Volumes/{v.catalog_name}/{v.schema_name}/{v.name}"
        print(f"    {root}", file=sys.stderr)
        _walk(w, v, root, ext_filter, files)
    return files


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def _save(name: str, items: list[dict], output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    base = os.path.join(output_dir, f"{name}_volume_files")
    with open(f"{base}.json", "w", encoding="utf-8") as f:
        json.dump(items, f, indent=2, default=str, ensure_ascii=False)
    if items:
        with open(f"{base}.csv", "w", newline="", encoding="utf-8-sig") as f:
            wr = csv.DictWriter(f, fieldnames=list(items[0].keys()))
            wr.writeheader()
            wr.writerows(items)
    else:
        open(f"{base}.csv", "w", encoding="utf-8-sig").close()
    print(f"  -> saved {base}.csv  ({len(items)} files)", file=sys.stderr)


def _print_summary(name: str, files: list[dict]) -> None:
    by_volume: dict[str, int] = {}
    by_ext: dict[str, int] = {}
    total_bytes = 0
    for f in files:
        by_volume[f["full_volume_name"]] = by_volume.get(f["full_volume_name"], 0) + 1
        by_ext[f["file_extension"] or "(none)"] = by_ext.get(f["file_extension"] or "(none)", 0) + 1
        total_bytes += int(f.get("file_size_bytes") or 0)

    print(f"\n{'═' * 72}", file=sys.stderr)
    print(f"  Summary — {name}  ({len(files)} files, {total_bytes / 1e6:.1f} MB)", file=sys.stderr)
    print(f"{'═' * 72}", file=sys.stderr)
    for vol, n in sorted(by_volume.items(), key=lambda x: -x[1]):
        print(f"  {vol:<60} {n:>6}", file=sys.stderr)
    if by_ext:
        print(f"{'─' * 72}", file=sys.stderr)
        print("  By extension:", file=sys.stderr)
        for ext, n in sorted(by_ext.items(), key=lambda x: -x[1]):
            print(f"    .{ext:<10} {n:>6}", file=sys.stderr)


def run(name: str, w: WorkspaceClient, args: argparse.Namespace) -> list[dict]:
    print(f"\n{'━' * 72}", file=sys.stderr)
    print(f"  Workspace : {name}", file=sys.stderr)
    print(f"  Host      : {w.config.host}", file=sys.stderr)
    print(f"{'━' * 72}", file=sys.stderr)
    files = collect(
        w,
        args.catalog,
        args.schema,
        args.volume,
        (args.extension or "").lower(),
        args.managed_only,
    )
    _print_summary(name, files)
    if args.save or args.config:
        _save(name, files, os.path.join(args.output_dir, name))
    return files


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Corteva MIC — UC volume artifacts inventory",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--config",     help="Path to workspaces JSON config (multi-workspace)")
    parser.add_argument("--host",       default=os.environ.get("DATABRICKS_HOST", ""))
    parser.add_argument("--token",      default=os.environ.get("DATABRICKS_TOKEN", ""))
    parser.add_argument("--profile",    default="")
    parser.add_argument("--catalog",    default="", help="Limit to one catalog")
    parser.add_argument("--schema",     default="", help="Limit to one schema (requires --catalog)")
    parser.add_argument("--volume",     default="", help="Single volume full name: catalog.schema.name")
    parser.add_argument("--extension",  default="", help="Filter by file extension (e.g. pkl)")
    parser.add_argument("--managed-only", action="store_true",
                        help="Only walk MANAGED volumes (skip EXTERNAL — typically raw data lakes)")
    parser.add_argument("--save",       action="store_true", help="Write CSV+JSON to disk")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                        help=f"Output root (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--json",       action="store_true", help="Print JSON to stdout")
    args = parser.parse_args()

    if args.schema and not args.catalog and not args.volume:
        parser.error("--schema requires --catalog (or use --volume <full_name>)")

    # ── Multi-workspace mode ──────────────────────────────────────────────────
    if args.config:
        with open(args.config) as f:
            workspaces = json.load(f)
        for ws in workspaces:
            name    = ws.get("name", "unknown")
            host    = (ws.get("host", "") or "").strip()
            token   = (ws.get("token", "") or "").strip()
            profile = (ws.get("profile", "") or "").strip()
            if not host or (not token and not profile):
                print(f"  [{name}] skipped — missing host/token/profile", file=sys.stderr)
                continue
            try:
                w = _make_client(host=host, token=token, profile=profile)
                run(name, w, args)
            except Exception as exc:  # noqa: BLE001
                print(f"  [{name}] failed: {exc}", file=sys.stderr)
        return

    # ── Single-workspace mode ─────────────────────────────────────────────────
    if not args.host and not args.profile:
        parser.error(
            "Provide --config, or one of: --host + --token, --profile, "
            "or set DATABRICKS_HOST / DATABRICKS_TOKEN env vars."
        )

    w = _make_client(host=args.host, token=args.token, profile=args.profile)
    name = (
        args.host.split("//")[-1].split(".")[0]
        if args.host
        else (args.profile or "workspace")
    )
    files = run(name, w, args)

    if args.json:
        print(json.dumps(files, indent=2, default=str))
    elif not args.save:
        for f in files:
            size = f["file_size_bytes"] or 0
            print(f"  {f['file_path']}  ({size} bytes, {f['modification_time']})")


if __name__ == "__main__":
    main()
