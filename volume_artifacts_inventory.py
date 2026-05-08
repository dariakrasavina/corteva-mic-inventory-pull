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

EXTERNAL volumes are always skipped — they're typically raw data-lake mounts
with millions of files, and ML artifacts only live in MANAGED volumes.

The catalog defaults to `mic_prod` (the Corteva MIC production catalog).
Pass `--catalog OTHER` to override.

─── Scope filters (combine any) ──────────────────────────────────────────────
    --volume CATALOG.SCHEMA.NAME    Single volume (fastest)
    --catalog NAME                  Limit to one catalog (default: mic_prod)
    --schema NAME                   Limit to one schema (within --catalog)
    --extension EXT                 Keep only files with this extension (e.g. pkl)
    (no filters)                    Walks every MANAGED volume in mic_prod

─── Hang protection ──────────────────────────────────────────────────────────
    --list-timeout SEC      Per-directory listing timeout (default 60s).
                            Wraps `list(iterator)` materialization, not the
                            bare SDK call — necessary because
                            files.list_directory_contents() returns a paginating
                            generator and a timeout on the call alone is a no-op.
    --volume-timeout SEC    Cumulative budget per volume (default 600s). Once
                            spent, the volume is abandoned (whatever was
                            captured is kept) and the next volume starts.

─── Examples ─────────────────────────────────────────────────────────────────
    # All MANAGED volumes in mic_prod, .pkl artifacts only
    python volume_artifacts_inventory.py --profile mic-prod \\
        --extension pkl --save

    # One specific schema in mic_prod
    python volume_artifacts_inventory.py --profile mic-prod \\
        --schema gold_seed_forecasting --save

    # One specific volume (catalog can be anything via the full name)
    python volume_artifacts_inventory.py --profile mic-prod \\
        --volume mic_prod.gold_seed_forecasting.artifacts \\
        --extension pkl --save

    # Multi-workspace via config (still defaults to mic_prod everywhere)
    python volume_artifacts_inventory.py --config workspaces.json --extension pkl

─── workspaces.json format ───────────────────────────────────────────────────
    [
      {"name": "dev",  "host": "https://adb-xxx.net", "profile": "dev-profile"},
      {"name": "prod", "host": "https://adb-zzz.net", "token":   "dapi..."}
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


DEFAULT_OUTPUT_DIR = os.path.expanduser("~/corteva-mic-workspace-assets/output")
DEFAULT_LIST_TIMEOUT_SEC = 60         # wall-clock budget per directory listing
DEFAULT_VOLUME_TIMEOUT_SEC = 600      # cumulative wall-clock budget per volume


class _VolumeBudgetExceeded(Exception):
    """Raised inside _walk when the per-volume time budget is spent."""


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
) -> list:
    """Resolve --volume / --catalog / --schema flags into the set of volumes to walk.

    EXTERNAL volumes are always excluded — they're typically raw data-lake mounts
    that don't hold ML artifacts and can have millions of files.
    """
    if volume_full_name:
        v = _safe(
            f"volumes.read({volume_full_name})",
            lambda: w.volumes.read(name=volume_full_name),
        )
        if not v:
            return []
        if _volume_type(v) != "MANAGED":
            print(
                f"    [SKIP] {volume_full_name} is {_volume_type(v)} — "
                f"EXTERNAL volumes are not walked",
                file=sys.stderr,
            )
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

    before = len(volumes)
    volumes = [v for v in volumes if _volume_type(v) == "MANAGED"]
    skipped = before - len(volumes)
    if skipped:
        print(f"  [INFO] excluded {skipped} EXTERNAL volume(s)", file=sys.stderr)
    return volumes


def _list_with_timeout(w: WorkspaceClient, path: str, timeout: float) -> list | None:
    """Materialize files.list_directory_contents with a wall-clock timeout.

    Wrapping `list(iterator)` (not just the SDK call) is critical: the SDK
    method returns a paginating generator, so the actual API work happens
    during iteration. A timeout on the bare call would fire on a no-op.

    Returns the entries list on success, or None on timeout.
    """
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    try:
        return pool.submit(
            lambda: list(w.files.list_directory_contents(directory_path=path))
        ).result(timeout=timeout)
    except concurrent.futures.TimeoutError:
        return None
    except PermissionDenied:
        print(f"    [WARN] permission denied: {path}", file=sys.stderr)
        return []
    except (NotFound, ResourceDoesNotExist):
        return []
    except Exception as exc:  # noqa: BLE001
        print(f"    [WARN] files.list_directory_contents({path}): {exc}", file=sys.stderr)
        return []
    finally:
        pool.shutdown(wait=False)


def _walk(
    w: WorkspaceClient,
    v: Any,
    path: str,
    ext_filter: str,
    sink: list[dict],
    list_timeout: float,
    volume_deadline: float,
) -> None:
    if time.monotonic() > volume_deadline:
        raise _VolumeBudgetExceeded(path)

    print(f"    listing {path}", file=sys.stderr)
    entries = _list_with_timeout(w, path, list_timeout)
    if entries is None:
        print(f"    [TIMEOUT] {path} (>{list_timeout}s) — skipped", file=sys.stderr)
        return

    for e in entries:
        if time.monotonic() > volume_deadline:
            raise _VolumeBudgetExceeded(path)
        epath = e.path or ""
        ename = e.name or os.path.basename(epath)
        if getattr(e, "is_directory", False):
            if epath:
                _walk(w, v, epath, ext_filter, sink, list_timeout, volume_deadline)
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
    list_timeout: float,
    volume_timeout: float,
) -> list[dict]:
    volumes = _list_target_volumes(w, catalog, schema, volume_full_name)
    print(f"  Walking {len(volumes)} volume(s)  "
          f"(per-listing timeout: {list_timeout:.0f}s, per-volume budget: {volume_timeout:.0f}s)",
          file=sys.stderr)
    files: list[dict] = []
    for v in volumes:
        root = f"/Volumes/{v.catalog_name}/{v.schema_name}/{v.name}"
        print(f"  ━━ {root}", file=sys.stderr)
        deadline = time.monotonic() + volume_timeout
        before = len(files)
        try:
            _walk(w, v, root, ext_filter, files, list_timeout, deadline)
        except _VolumeBudgetExceeded as exc:
            print(
                f"  [BUDGET] {root} — exceeded {volume_timeout:.0f}s, abandoning at {exc} "
                f"(captured {len(files) - before} files before bailout)",
                file=sys.stderr,
            )
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
        args.list_timeout,
        args.volume_timeout,
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
    parser.add_argument("--catalog",    default="mic_prod",
                        help="Catalog to scan (default: mic_prod). Pass an empty string "
                             "(--catalog '') to walk all catalogs.")
    parser.add_argument("--schema",     default="", help="Limit to one schema within --catalog")
    parser.add_argument("--volume",     default="", help="Single volume full name: catalog.schema.name")
    parser.add_argument("--extension",  default="", help="Filter by file extension (e.g. pkl)")
    parser.add_argument("--list-timeout", type=float, default=DEFAULT_LIST_TIMEOUT_SEC,
                        help=f"Wall-clock timeout per directory listing in seconds "
                             f"(default: {DEFAULT_LIST_TIMEOUT_SEC}). "
                             f"On timeout, that directory is skipped and the walk continues.")
    parser.add_argument("--volume-timeout", type=float, default=DEFAULT_VOLUME_TIMEOUT_SEC,
                        help=f"Cumulative time budget per volume in seconds "
                             f"(default: {DEFAULT_VOLUME_TIMEOUT_SEC}). "
                             f"Once spent, the entire volume is abandoned and the next one starts.")
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
