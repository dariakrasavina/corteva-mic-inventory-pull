#!/usr/bin/env python3
"""
Corteva MIC — Workspace Configuration Inventory  (SDK version)
==============================================================
Captures workspace-level settings and configuration resources using the
Databricks Python SDK. Covers three complementary settings APIs
(legacy workspace_conf, workspace_settings_v2, typed w.settings),
plus identity (users, groups, service principals), compute (clusters,
policies, pools, SQL warehouses), Unity Catalog objects (external
locations, storage credentials, connections), SQL assets (queries, alerts),
and platform resources (init scripts, IP access lists, secret scopes, tokens).

Uses the same auth, CLI, and output patterns as workspace_inventory_sdk.py.

─── Authentication options ──────────────────────────────────────────────────
    PAT token:         --host URL --token dapi...
    CLI profile:       --profile my-profile   (reads ~/.databrickscfg)
    Env vars:          DATABRICKS_HOST + DATABRICKS_TOKEN
    Service principal: DATABRICKS_HOST + DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET

─── Single workspace ────────────────────────────────────────────────────────
    python workspace_config_inventory_sdk.py --host https://adb-xxx.net --token dapi...
    python workspace_config_inventory_sdk.py --profile my-profile
    python workspace_config_inventory_sdk.py --profile my-profile --section workspace_conf_legacy

─── Multiple workspaces (reads workspaces.json) ─────────────────────────────
    python workspace_config_inventory_sdk.py --config workspaces.json

─── Options ─────────────────────────────────────────────────────────────────
    --config FILE       Path to workspaces JSON config (runs all workspaces)
    --host URL          Single workspace host URL
    --token TOKEN       Single workspace PAT token
    --profile NAME      ~/.databrickscfg profile name
    --save              Write JSON + CSV files to disk (always on with --config)
    --output-dir DIR    Root output directory (default: ~/corteva-mic-workspace-assets/output)
    --section KEY       Collect one section only
    --json              Print JSON to stdout (single workspace only)
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
        return WorkspaceClient(host=host)
    return WorkspaceClient()


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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _warn(msg: str) -> None:
    print(f"    [WARN] {msg}", file=sys.stderr)


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


def collect_users(c: InventoryCollector) -> list[dict]:
    """Workspace users."""
    raw = c.safe("users.list", lambda: list(c.w.users.list()), default=[])
    return [
        {
            "id":           u.id,
            "user_name":    u.user_name,
            "display_name": u.display_name,
            "active":       u.active,
        }
        for u in raw
    ]


def collect_groups(c: InventoryCollector) -> list[dict]:
    """Workspace groups with member counts."""
    raw = c.safe("groups.list", lambda: list(c.w.groups.list()), default=[])
    items: list[dict] = []
    for g in raw:
        members = [m.display for m in (g.members or [])]
        items.append({
            "id":           g.id,
            "display_name": g.display_name,
            "member_count": len(members),
            "members":      json.dumps(members),
        })
    return items


def collect_service_principals(c: InventoryCollector) -> list[dict]:
    """Service principals."""
    raw = c.safe("service_principals.list", lambda: list(c.w.service_principals.list()), default=[])
    return [
        {
            "id":             sp.id,
            "application_id": sp.application_id,
            "display_name":   sp.display_name,
            "active":         sp.active,
        }
        for sp in raw
    ]


def collect_clusters(c: InventoryCollector) -> list[dict]:
    """All-purpose and job clusters."""
    raw = c.safe("clusters.list", lambda: list(c.w.clusters.list()), default=[])
    return [
        {
            "cluster_id":              cl.cluster_id,
            "cluster_name":            cl.cluster_name,
            "state":                   _val(cl.state),
            "spark_version":           cl.spark_version,
            "node_type_id":            cl.node_type_id,
            "driver_node_type_id":     cl.driver_node_type_id,
            "autoscale":               str(cl.autoscale) if cl.autoscale else None,
            "num_workers":             cl.num_workers,
            "autotermination_minutes": cl.autotermination_minutes,
            "spark_conf":              json.dumps(dict(cl.spark_conf)) if cl.spark_conf else None,
            "custom_tags":             json.dumps(dict(cl.custom_tags)) if cl.custom_tags else None,
            "data_security_mode":      _val(cl.data_security_mode),
            "single_user_name":        cl.single_user_name,
        }
        for cl in raw
    ]


def collect_cluster_policies(c: InventoryCollector) -> list[dict]:
    """Cluster policies."""
    raw = c.safe("cluster_policies.list", lambda: list(c.w.cluster_policies.list()), default=[])
    return [
        {
            "policy_id":   p.policy_id,
            "name":        p.name,
            "description": p.description,
            "is_default":  p.is_default,
        }
        for p in raw
    ]


def collect_instance_pools(c: InventoryCollector) -> list[dict]:
    """Instance pools."""
    raw = c.safe("instance_pools.list", lambda: list(c.w.instance_pools.list()), default=[])
    return [
        {
            "instance_pool_id":   pool.instance_pool_id,
            "instance_pool_name": pool.instance_pool_name,
            "node_type_id":       pool.node_type_id,
            "min_idle_instances":  pool.min_idle_instances,
            "max_capacity":        pool.max_capacity,
            "idle_instance_autotermination_minutes": pool.idle_instance_autotermination_minutes,
        }
        for pool in raw
    ]


def collect_sql_warehouses(c: InventoryCollector) -> list[dict]:
    """SQL warehouses."""
    raw = c.safe("warehouses.list", lambda: list(c.w.warehouses.list()), default=[])
    return [
        {
            "id":                       wh.id,
            "name":                     wh.name,
            "cluster_size":             wh.cluster_size,
            "min_num_clusters":         wh.min_num_clusters,
            "max_num_clusters":         wh.max_num_clusters,
            "auto_stop_mins":           wh.auto_stop_mins,
            "warehouse_type":           _val(wh.warehouse_type),
            "enable_serverless_compute": wh.enable_serverless_compute,
            "state":                    _val(wh.state),
            "spot_instance_policy":     _val(wh.spot_instance_policy),
            "channel":                  str(wh.channel) if wh.channel else None,
        }
        for wh in raw
    ]


def collect_external_locations(c: InventoryCollector) -> list[dict]:
    """Unity Catalog external locations."""
    raw = c.safe("external_locations.list", lambda: list(c.w.external_locations.list()), default=[])
    return [
        {
            "name":            el.name,
            "url":             el.url,
            "credential_name": el.credential_name,
            "owner":           el.owner,
            "read_only":       el.read_only,
            "comment":         el.comment,
        }
        for el in raw
    ]


def collect_storage_credentials(c: InventoryCollector) -> list[dict]:
    """Unity Catalog storage credentials."""
    raw = c.safe("storage_credentials.list", lambda: list(c.w.storage_credentials.list()), default=[])
    return [
        {
            "name":                    sc.name,
            "owner":                   sc.owner,
            "read_only":               sc.read_only,
            "comment":                 sc.comment,
            "used_for_managed_storage": sc.used_for_managed_storage,
        }
        for sc in raw
    ]


def collect_connections(c: InventoryCollector) -> list[dict]:
    """Unity Catalog connections (Lakehouse Federation)."""
    raw = c.safe("connections.list", lambda: list(c.w.connections.list()), default=[])
    return [
        {
            "name":            conn.name,
            "connection_type": _val(conn.connection_type),
            "owner":           conn.owner,
            "comment":         conn.comment,
        }
        for conn in raw
    ]


def collect_sql_queries(c: InventoryCollector) -> list[dict]:
    """Saved SQL queries."""
    raw = c.safe("queries.list", lambda: list(c.w.queries.list()), default=[])
    return [
        {
            "id":              q.id,
            "display_name":    q.display_name,
            "owner_user_name": q.owner_user_name,
            "warehouse_id":    q.warehouse_id,
        }
        for q in raw
    ]


def collect_sql_alerts(c: InventoryCollector) -> list[dict]:
    """SQL alerts."""
    raw = c.safe("alerts.list", lambda: list(c.w.alerts.list()), default=[])
    return [
        {
            "id":              a.id,
            "display_name":    a.display_name,
            "owner_user_name": a.owner_user_name,
            "state":           _val(a.state),
        }
        for a in raw
    ]


def collect_workspace_conf_legacy(c: InventoryCollector) -> list[dict]:
    """Legacy workspace_conf key-value settings (no discovery endpoint).

    All known keys — union of Terraform exporter (37 keys) and SDK notebook
    discoveries. This API has no list/discovery endpoint, so keys must be
    enumerated explicitly.
    """
    known_keys = [
        # Security & Access Control
        "enableIpAccessLists",
        "enableTokensConfig",
        "maxTokenLifetimeDays",
        "maxUserInactiveDays",
        "enableWebTerminal",
        "enforceUserIsolation",
        "customerApprovedWSLoginExpirationTime",
        # Access Control Lists
        "enableJobViewAcls",
        "enforceWorkspaceViewAcls",
        "enforceClusterViewAcls",
        # Security Headers
        "enable-X-Frame-Options",
        "enable-X-Content-Type-Options",
        "enable-X-XSS-Protection",
        # IMDSv2 enforcement (AWS)
        "enableEnforceImdsV2",
        # Notebook & UI Features
        "enableExportNotebook",
        "enableNotebookTableClipboard",
        "enableResultsDownloading",
        "enableUploadDataUis",
        "enableDbfsFileBrowser",
        "enableLegacyNotebookVisualizations",
        # Compute
        "enableDcs",
        "enableProjectTypeInWorkspace",
        "enableGp3",
        "enableLibraryAndInitScriptOnSharedCluster",
        # Projects & Repos
        "enableProjectsAllowList",
        "projectsAllowList",
        "reposIpynbResultsExportPermissions",
        # Deprecated features
        "enableDeprecatedClusterNamedInitScripts",
        "enableDeprecatedGlobalInitScripts",
        # ML & MLflow
        "enableDatabricksAutologgingAdminConf",
        "mlflowRunArtifactDownloadEnabled",
        "mlflowModelServingEndpointCreationEnabled",
        "mlflowModelRegistryEmailNotificationsEnabled",
        # RStudio
        "rStudioUserDefaultHomeBase",
        # Pipelines
        "enablePipelinesDataSample",
        # Audit & Compliance
        "enableVerboseAuditLogs",
        # Storage
        "enableWorkspaceFilesystem",
        "storeInteractiveNotebookResultsInCustomerAccount",
    ]
    items: list[dict] = []
    for key in known_keys:
        result = c.safe(
            f"workspace_conf.get_status({key})",
            lambda k=key: c.w.workspace_conf.get_status(keys=k),
        )
        if result:
            for k, v in result.items():
                items.append({"key": k, "value": str(v)})
    return items


def collect_workspace_settings_v2(c: InventoryCollector) -> list[dict]:
    """V2 workspace settings — uses discovery endpoint for 100+ feature flags."""
    metadata = c.safe(
        "workspace_settings_v2.list_metadata",
        lambda: list(c.w.workspace_settings_v2.list_workspace_settings_metadata()),
        default=[],
    )
    items: list[dict] = []
    for m in sorted(metadata, key=lambda x: x.name):
        current_val = None
        try:
            result = c.w.workspace_settings_v2.get_public_workspace_setting(name=m.name)
            current_val = str(getattr(result, "setting_value", None))
        except Exception:  # noqa: BLE001
            current_val = None
        items.append({
            "setting_name":  m.name,
            "current_value": current_val,
            "setting_type":  getattr(m, "type", None),
            "description":   (getattr(m, "description", None) or "")[:200],
        })
    return items


def collect_workspace_settings_typed(c: InventoryCollector) -> list[dict]:
    """Typed w.settings sub-APIs — strongly-typed settings with get/update."""
    api_names = [m for m in dir(c.w.settings) if not m.startswith("_")]
    items: list[dict] = []
    for api_name in sorted(api_names):
        api = getattr(c.w.settings, api_name)
        has_update = hasattr(api, "update")
        if hasattr(api, "get"):
            result = c.safe(
                f"settings.{api_name}.get",
                lambda a=api: a.get(),
            )
            items.append({
                "setting_name":  api_name,
                "current_value": str(result)[:500] if result is not None else None,
                "has_update":    has_update,
            })
        else:
            items.append({
                "setting_name":  api_name,
                "current_value": None,
                "has_update":    has_update,
            })
    return items


def collect_sql_global_config(c: InventoryCollector) -> list[dict]:
    """SQL global configuration — security policy, serverless, data access."""
    data = c.safe(
        "sql_global_config",
        lambda: c.w.api_client.do("GET", "/api/2.0/sql/config/warehouses"),
    )
    if not data:
        return []
    # Flatten config into key-value rows for consistent CSV output
    items: list[dict] = []
    for k, v in sorted(data.items()):
        items.append({"key": k, "value": json.dumps(v) if isinstance(v, (dict, list)) else str(v)})
    return items


def collect_global_init_scripts(c: InventoryCollector) -> list[dict]:
    """Global init scripts — metadata and content."""
    raw = c.safe(
        "global_init_scripts.list",
        lambda: list(c.w.global_init_scripts.list()),
        default=[],
    )
    items: list[dict] = []
    for gi in raw:
        script_content = None
        detail = c.safe(
            f"global_init_scripts.get({gi.script_id})",
            lambda sid=gi.script_id: c.w.global_init_scripts.get(sid),
        )
        if detail:
            script_content = getattr(detail, "script", None)
        items.append({
            "script_id":      gi.script_id,
            "name":           gi.name,
            "enabled":        gi.enabled,
            "position":       gi.position,
            "script_content": script_content,
        })
    return items


def collect_ip_access_lists(c: InventoryCollector) -> list[dict]:
    """IP access lists."""
    raw = c.safe(
        "ip_access_lists.list",
        lambda: list(c.w.ip_access_lists.list()),
        default=[],
    )
    return [
        {
            "list_id":      ipl.list_id,
            "label":        ipl.label,
            "list_type":    _val(ipl.list_type),
            "ip_addresses": json.dumps(ipl.ip_addresses) if ipl.ip_addresses else None,
            "enabled":      ipl.enabled,
        }
        for ipl in raw
    ]


def collect_secret_scopes(c: InventoryCollector) -> list[dict]:
    """Secret scopes — lists scopes and key names, NOT secret values."""
    scopes = c.safe(
        "secrets.list_scopes",
        lambda: list(c.w.secrets.list_scopes()),
        default=[],
    )
    items: list[dict] = []
    for scope in scopes:
        keys = c.safe(
            f"secrets.list_secrets({scope.name})",
            lambda s=scope.name: list(c.w.secrets.list_secrets(scope=s)),
            default=[],
        )
        items.append({
            "scope_name":   scope.name,
            "backend_type": _val(scope.backend_type),
            "secret_count": len(keys),
            "secret_keys":  json.dumps([k.key for k in keys]),
        })
    return items


def collect_tokens(c: InventoryCollector) -> list[dict]:
    """Managed personal access tokens (requires admin)."""
    raw = c.safe(
        "token_management.list",
        lambda: list(c.w.token_management.list()),
        default=[],
    )
    return [
        {
            "token_id":            tok.token_id,
            "created_by_username": tok.created_by_username,
            "comment":             tok.comment,
            "expiry_time":         tok.expiry_time,
        }
        for tok in raw
    ]


# ---------------------------------------------------------------------------
# Section registry
# ---------------------------------------------------------------------------

SECTIONS = [
    # Identity & Access
    ("users",                    "Users",                        collect_users),
    ("groups",                   "Groups",                       collect_groups),
    ("service_principals",       "Service Principals",           collect_service_principals),
    # Compute
    ("clusters",                 "Clusters",                     collect_clusters),
    ("cluster_policies",         "Cluster Policies",             collect_cluster_policies),
    ("instance_pools",           "Instance Pools",               collect_instance_pools),
    ("sql_warehouses",           "SQL Warehouses",               collect_sql_warehouses),
    # Unity Catalog
    ("external_locations",       "External Locations",           collect_external_locations),
    ("storage_credentials",      "Storage Credentials",          collect_storage_credentials),
    ("connections",              "Connections",                   collect_connections),
    # Workspace Settings
    ("workspace_conf_legacy",    "Workspace Settings (Legacy)",  collect_workspace_conf_legacy),
    ("workspace_settings_v2",    "Workspace Settings (V2)",      collect_workspace_settings_v2),
    ("workspace_settings_typed", "Workspace Settings (Typed)",   collect_workspace_settings_typed),
    ("sql_global_config",        "SQL Global Config",            collect_sql_global_config),
    ("global_init_scripts",      "Global Init Scripts",          collect_global_init_scripts),
    ("ip_access_lists",          "IP Access Lists",              collect_ip_access_lists),
    ("secret_scopes",            "Secret Scopes",                collect_secret_scopes),
    ("tokens",                   "Managed Tokens",               collect_tokens),
    # SQL Assets
    ("sql_queries",              "SQL Queries",                  collect_sql_queries),
    ("sql_alerts",               "SQL Alerts",                   collect_sql_alerts),
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
        description="Corteva MIC — Workspace configuration inventory (SDK-based)",
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
        print(f"\nTotal: {sum(len(v) for v in inventory.values())} settings")


if __name__ == "__main__":
    main()
