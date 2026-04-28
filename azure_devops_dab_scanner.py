#!/usr/bin/env python3
"""
Corteva MIC — Azure DevOps DAB Scanner
=======================================
Scans Azure DevOps repositories for Databricks Asset Bundle (DAB)
configuration files (databricks.yml) and extracts what resources each
bundle defines and what notebooks/files they reference.

This gives you job and notebook lineage: which bundle deploys which job,
and which notebooks each job task or DLT pipeline references.

─── Outputs ─────────────────────────────────────────────────────────────────
    <repo>_dab_bundles.csv/.json            — one row per bundle
    <repo>_dab_job_tasks.csv/.json          — one row per job task with notebook/file reference
    <repo>_dab_pipeline_notebooks.csv/.json — one row per DLT pipeline notebook library
    <repo>_dab_apps.csv/.json               — one row per Databricks App
    <repo>_dab_libraries.csv/.json          — one row per library dependency across all job tasks
    <repo>_dab_workspace_targets.csv/.json  — one row per target/environment per bundle

─── Single repo ─────────────────────────────────────────────────────────────
    python azure_devops_dab_scanner.py \
        --org vs-pioneer --project project0 \
        --repo Sales-MarketInsightsCloud \
        --token <ADO-PAT> --save

─── All repos in the project ────────────────────────────────────────────────
    python azure_devops_dab_scanner.py \
        --org vs-pioneer --project project0 \
        --token <ADO-PAT> --save

─── Options ─────────────────────────────────────────────────────────────────
    --org ORG           Azure DevOps org name (e.g. vs-pioneer)
    --project PROJECT   Azure DevOps project name (e.g. project0)
    --repo REPO         Single repo name. Omit to scan all repos in the project.
    --token TOKEN       Azure DevOps PAT token (or set ADO_TOKEN env var)
    --save              Write JSON + CSV files to disk
    --output-dir DIR    Root output directory (default: ~/corteva-mic-workspace-assets/output)
    --json              Print JSON to stdout
"""

from __future__ import annotations

import argparse
import base64
import csv
import fnmatch
import json
import os
import sys
import urllib.parse
import urllib.request
from typing import Any

try:
    import yaml
except ImportError:
    print("ERROR: pyyaml is required. Install with: pip install pyyaml", file=sys.stderr)
    sys.exit(1)


DEFAULT_OUTPUT_DIR = os.path.expanduser("~/corteva-mic-workspace-assets/output")


# ---------------------------------------------------------------------------
# Azure DevOps REST client
# ---------------------------------------------------------------------------


class ADOClient:
    def __init__(self, org: str, project: str, token: str) -> None:
        self.org = org
        self.project = project
        # Support both visualstudio.com (legacy) and dev.azure.com (modern).
        # If org contains no dot, assume visualstudio.com subdomain format.
        if "." in org:
            self._base = f"https://{org}/{urllib.parse.quote(project)}/_apis/git"
        else:
            self._base = f"https://{org}.visualstudio.com/{urllib.parse.quote(project)}/_apis/git"
        self._auth = base64.b64encode((":" + token).encode()).decode()

    def _get_json(self, url: str) -> Any:
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {self._auth}"})
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read().decode())

    def _get_text(self, url: str) -> str:
        req = urllib.request.Request(url, headers={"Authorization": f"Basic {self._auth}"})
        with urllib.request.urlopen(req) as r:
            return r.read().decode()

    def list_repos(self) -> list[str]:
        data = self._get_json(f"{self._base}/repositories?api-version=7.0")
        return [r["name"] for r in data.get("value", [])]

    def list_files(self, repo: str) -> list[str]:
        url = (
            f"{self._base}/repositories/{urllib.parse.quote(repo)}/items"
            f"?scopePath=/&recursionLevel=Full&api-version=7.0"
        )
        data = self._get_json(url)
        return [i["path"] for i in data.get("value", []) if i.get("gitObjectType") == "blob"]

    def get_file(self, repo: str, path: str) -> str:
        url = (
            f"{self._base}/repositories/{urllib.parse.quote(repo)}/items"
            f"?path={urllib.parse.quote(path)}&api-version=7.0"
        )
        return self._get_text(url)


# ---------------------------------------------------------------------------
# YAML helpers
# ---------------------------------------------------------------------------


def _load_yaml(text: str, source: str = "") -> dict:
    try:
        return yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        print(f"    [WARN] YAML parse error in {source}: {exc}", file=sys.stderr)
        return {}


def _resolve_includes(
    bundle_dir: str,
    include_patterns: list[str],
    all_files: list[str],
    ado: ADOClient,
    repo: str,
) -> list[dict]:
    """Resolve include glob patterns relative to bundle_dir and return parsed YAMLs."""
    configs: list[dict] = []
    for pattern in include_patterns:
        # Normalise: strip leading ./ or /
        if pattern.startswith("./"):
            pattern = pattern[2:]
        elif pattern.startswith("/"):
            pattern = pattern[1:]
        abs_pattern = bundle_dir.rstrip("/") + "/" + pattern
        matches = sorted(f for f in all_files if fnmatch.fnmatch(f, abs_pattern))
        for fpath in matches:
            try:
                content = ado.get_file(repo, fpath)
                parsed = _load_yaml(content, fpath)
                if parsed:
                    configs.append(parsed)
            except Exception as exc:  # noqa: BLE001
                print(f"    [WARN] Could not read include {fpath}: {exc}", file=sys.stderr)
    return configs


# ---------------------------------------------------------------------------
# Bundle scanner
# ---------------------------------------------------------------------------


def scan_bundle(
    ado: ADOClient,
    repo: str,
    bundle_yaml_path: str,
    all_files: list[str],
) -> dict | None:
    """Parse a databricks.yml and all its included resource files into a bundle dict."""
    bundle_dir = os.path.dirname(bundle_yaml_path)

    try:
        content = ado.get_file(repo, bundle_yaml_path)
    except Exception as exc:  # noqa: BLE001
        print(f"    [WARN] Could not read {bundle_yaml_path}: {exc}", file=sys.stderr)
        return None

    config = _load_yaml(content, bundle_yaml_path)
    if not config:
        return None

    bundle_name = (config.get("bundle") or {}).get("name") or os.path.basename(bundle_dir)

    # Resolve and parse included resource files
    raw_includes = config.get("include") or []
    if isinstance(raw_includes, str):
        raw_includes = [raw_includes]
    included = _resolve_includes(bundle_dir, raw_includes, all_files, ado, repo)

    # Merge resources from main config + all includes
    all_resources: dict[str, dict] = {}
    for cfg in [config, *included]:
        for rtype, rdict in (cfg.get("resources") or {}).items():
            if isinstance(rdict, dict):
                all_resources.setdefault(rtype, {}).update(rdict)

    # Extract environments and workspace hosts from targets
    targets = config.get("targets") or {}
    environments = list(targets.keys())
    workspace_hosts: list[str] = []
    for tval in targets.values():
        host = ((tval or {}).get("workspace") or {}).get("host")
        if host and host not in workspace_hosts:
            workspace_hosts.append(host)

    return {
        "repo":            repo,
        "bundle_name":     bundle_name,
        "bundle_path":     bundle_yaml_path,
        "bundle_dir":      bundle_dir,
        "environments":    environments,
        "workspace_hosts": workspace_hosts,
        "targets":         targets,
        "resources":       all_resources,
    }


# ---------------------------------------------------------------------------
# Row extractors
# ---------------------------------------------------------------------------


def extract_bundle_row(bundle: dict) -> dict:
    resources = bundle["resources"]
    return {
        "repo":            bundle["repo"],
        "bundle_path":     bundle["bundle_path"],
        "bundle_name":     bundle["bundle_name"],
        "environments":    ", ".join(bundle["environments"]),
        "workspace_hosts": ", ".join(bundle["workspace_hosts"]),
        "job_count":       len(resources.get("jobs") or {}),
        "pipeline_count":  len(resources.get("pipelines") or {}),
        "app_count":       len(resources.get("apps") or {}),
    }


def extract_job_tasks(bundle: dict) -> list[dict]:
    rows: list[dict] = []
    for job_key, job in (bundle["resources"].get("jobs") or {}).items():
        if not isinstance(job, dict):
            continue
        job_name = job.get("name", job_key)
        schedule = (job.get("schedule") or {}).get("quartz_cron_expression", "")
        for task in (job.get("tasks") or []):
            if not isinstance(task, dict):
                continue
            task_key = task.get("task_key", "")
            base: dict = {
                "repo":        bundle["repo"],
                "bundle_path": bundle["bundle_path"],
                "bundle_name": bundle["bundle_name"],
                "job_key":     job_key,
                "job_name":    job_name,
                "schedule":    schedule,
                "task_key":    task_key,
                "task_type":   "",
                "path":        "",
                "source":      "",
            }
            if "notebook_task" in task:
                nb = task["notebook_task"] or {}
                rows.append({**base,
                             "task_type": "notebook_task",
                             "path":      nb.get("notebook_path", ""),
                             "source":    nb.get("source", "WORKSPACE")})
            elif "spark_python_task" in task:
                sp = task["spark_python_task"] or {}
                rows.append({**base,
                             "task_type": "spark_python_task",
                             "path":      sp.get("python_file", "")})
            elif "pipeline_task" in task:
                pt = task["pipeline_task"] or {}
                rows.append({**base,
                             "task_type": "pipeline_task",
                             "path":      str(pt.get("pipeline_id", ""))})
            elif "run_job_task" in task:
                rj = task["run_job_task"] or {}
                rows.append({**base,
                             "task_type": "run_job_task",
                             "path":      str(rj.get("job_id", ""))})
            elif "dbt_task" in task:
                rows.append({**base, "task_type": "dbt_task"})
            elif "sql_task" in task:
                rows.append({**base, "task_type": "sql_task"})
            elif "spark_jar_task" in task:
                sj = task["spark_jar_task"] or {}
                rows.append({**base,
                             "task_type": "spark_jar_task",
                             "path":      sj.get("main_class_name", "")})
            else:
                detected = next((k for k in task if k.endswith("_task")), "unknown_task")
                rows.append({**base, "task_type": detected})
    return rows


def extract_pipeline_notebooks(bundle: dict) -> list[dict]:
    rows: list[dict] = []
    for pl_key, pipeline in (bundle["resources"].get("pipelines") or {}).items():
        if not isinstance(pipeline, dict):
            continue
        pl_name = pipeline.get("name", pl_key)
        catalog = pipeline.get("catalog", "")
        target  = pipeline.get("target", "")
        for lib in (pipeline.get("libraries") or []):
            if not isinstance(lib, dict):
                continue
            if "notebook" in lib:
                lib_type = "notebook"
                path = (lib["notebook"] or {}).get("path", "")
            elif "file" in lib:
                lib_type = "file"
                path = (lib["file"] or {}).get("path", "")
            else:
                lib_type = next(iter(lib), "unknown")
                path = str(next(iter(lib.values()), ""))
            rows.append({
                "repo":          bundle["repo"],
                "bundle_path":   bundle["bundle_path"],
                "bundle_name":   bundle["bundle_name"],
                "pipeline_key":  pl_key,
                "pipeline_name": pl_name,
                "catalog":       catalog,
                "target_schema": target,
                "library_type":  lib_type,
                "notebook_path": path,
            })
    return rows


def extract_apps(bundle: dict) -> list[dict]:
    rows: list[dict] = []
    for app_key, app in (bundle["resources"].get("apps") or {}).items():
        if not isinstance(app, dict):
            continue
        rows.append({
            "repo":             bundle["repo"],
            "bundle_path":      bundle["bundle_path"],
            "bundle_name":      bundle["bundle_name"],
            "app_key":          app_key,
            "app_name":         app.get("name", app_key),
            "source_code_path": app.get("source_code_path", ""),
            "description":      app.get("description", ""),
        })
    return rows


def extract_libraries(bundle: dict) -> list[dict]:
    """Extract all library dependencies declared on job task clusters."""
    rows: list[dict] = []
    for job_key, job in (bundle["resources"].get("jobs") or {}).items():
        if not isinstance(job, dict):
            continue
        job_name = job.get("name", job_key)
        for task in (job.get("tasks") or []):
            if not isinstance(task, dict):
                continue
            task_key = task.get("task_key", "")
            for lib in (task.get("libraries") or []):
                if not isinstance(lib, dict):
                    continue
                base: dict = {
                    "repo":        bundle["repo"],
                    "bundle_path": bundle["bundle_path"],
                    "bundle_name": bundle["bundle_name"],
                    "job_key":     job_key,
                    "job_name":    job_name,
                    "task_key":    task_key,
                }
                if "pypi" in lib:
                    pkg = ((lib["pypi"] or {}).get("package") or "")
                    # split "name==version" or "name>=version" into parts
                    for sep in ("==", ">=", "<=", "!=", "~="):
                        if sep in pkg:
                            name, _, ver = pkg.partition(sep)
                            break
                    else:
                        name, ver = pkg, ""
                    rows.append({**base,
                                 "library_type":    "pypi",
                                 "package":         pkg,
                                 "package_name":    name.strip(),
                                 "package_version": ver.strip()})
                elif "maven" in lib:
                    coords = ((lib["maven"] or {}).get("coordinates") or "")
                    rows.append({**base,
                                 "library_type":    "maven",
                                 "package":         coords,
                                 "package_name":    coords,
                                 "package_version": ""})
                elif "cran" in lib:
                    pkg = ((lib["cran"] or {}).get("package") or "")
                    ver = ((lib["cran"] or {}).get("repo") or "")
                    rows.append({**base,
                                 "library_type":    "cran",
                                 "package":         pkg,
                                 "package_name":    pkg,
                                 "package_version": ver})
                elif "egg" in lib:
                    rows.append({**base,
                                 "library_type":    "egg",
                                 "package":         str(lib["egg"]),
                                 "package_name":    str(lib["egg"]),
                                 "package_version": ""})
                elif "whl" in lib:
                    rows.append({**base,
                                 "library_type":    "whl",
                                 "package":         str(lib["whl"]),
                                 "package_name":    str(lib["whl"]),
                                 "package_version": ""})
                elif "jar" in lib:
                    rows.append({**base,
                                 "library_type":    "jar",
                                 "package":         str(lib["jar"]),
                                 "package_name":    str(lib["jar"]),
                                 "package_version": ""})
    return rows


def extract_workspace_targets(bundle: dict) -> list[dict]:
    """Extract all deployment targets (environments) and their workspace hosts."""
    rows: list[dict] = []
    for target_name, tval in (bundle.get("targets") or {}).items():
        tval = tval or {}
        ws_block = tval.get("workspace") or {}
        run_as = tval.get("run_as") or {}
        rows.append({
            "repo":                       bundle["repo"],
            "bundle_path":                bundle["bundle_path"],
            "bundle_name":                bundle["bundle_name"],
            "target_name":                target_name,
            "workspace_host":             ws_block.get("host", ""),
            "mode":                       tval.get("mode", ""),
            "is_default":                 str(tval.get("default", False)).lower(),
            "run_as_service_principal":   run_as.get("service_principal_name", ""),
        })
    return rows


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def _save(key: str, items: list[dict], prefix: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    base = os.path.join(output_dir, f"{prefix}_{key}")
    with open(f"{base}.json", "w") as f:
        json.dump(items, f, indent=2, default=str)
    if items:
        with open(f"{base}.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(items[0].keys()))
            w.writeheader()
            w.writerows(items)
    else:
        open(f"{base}.csv", "w").close()
    print(f"    -> saved {base}.csv", file=sys.stderr)


def _print_summary(
    repo: str,
    bundle_rows: list[dict],
    job_rows: list[dict],
    pipeline_rows: list[dict],
    app_rows: list[dict],
    lib_rows: list[dict],
    target_rows: list[dict],
) -> None:
    print(f"\n{'═' * 72}", file=sys.stderr)
    print(f"  Summary — {repo}", file=sys.stderr)
    print(f"{'═' * 72}", file=sys.stderr)
    print(f"  Bundles:             {len(bundle_rows)}", file=sys.stderr)
    print(f"  Job tasks:           {len(job_rows)}", file=sys.stderr)
    print(f"  Pipeline notebooks:  {len(pipeline_rows)}", file=sys.stderr)
    print(f"  Apps:                {len(app_rows)}", file=sys.stderr)
    print(f"  Library deps:        {len(lib_rows)}", file=sys.stderr)
    print(f"  Workspace targets:   {len(target_rows)}", file=sys.stderr)
    print(f"{'─' * 72}", file=sys.stderr)
    for b in bundle_rows:
        envs = b["environments"] or "(no targets defined)"
        hosts = b["workspace_hosts"] or ""
        print(f"  {b['bundle_name']:<45} envs: {envs}", file=sys.stderr)
        if hosts:
            print(f"  {'':45} host: {hosts}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Per-repo scan
# ---------------------------------------------------------------------------


def scan_repo(
    ado: ADOClient,
    repo: str,
    output_dir: str,
    save: bool,
) -> dict[str, list[dict]]:
    print(f"\n{'━' * 72}", file=sys.stderr)
    print(f"  Repo : {repo}", file=sys.stderr)
    print(f"{'━' * 72}", file=sys.stderr)

    print("  Listing files...", file=sys.stderr)
    try:
        all_files = ado.list_files(repo)
    except Exception as exc:  # noqa: BLE001
        print(f"  [ERROR] Could not list files: {exc}", file=sys.stderr)
        return {}

    bundle_files = sorted(f for f in all_files if f.endswith("databricks.yml"))
    print(f"  Found {len(bundle_files)} databricks.yml file(s)", file=sys.stderr)

    bundle_rows:   list[dict] = []
    job_rows:      list[dict] = []
    pipeline_rows: list[dict] = []
    app_rows:      list[dict] = []
    lib_rows:      list[dict] = []
    target_rows:   list[dict] = []

    for bundle_path in bundle_files:
        print(f"  Scanning {bundle_path}...", file=sys.stderr)
        bundle = scan_bundle(ado, repo, bundle_path, all_files)
        if not bundle:
            continue
        bundle_rows.append(extract_bundle_row(bundle))
        job_rows.extend(extract_job_tasks(bundle))
        pipeline_rows.extend(extract_pipeline_notebooks(bundle))
        app_rows.extend(extract_apps(bundle))
        lib_rows.extend(extract_libraries(bundle))
        target_rows.extend(extract_workspace_targets(bundle))

    _print_summary(repo, bundle_rows, job_rows, pipeline_rows, app_rows, lib_rows, target_rows)

    results: dict[str, list[dict]] = {
        "dab_bundles":              bundle_rows,
        "dab_job_tasks":            job_rows,
        "dab_pipeline_notebooks":   pipeline_rows,
        "dab_apps":                 app_rows,
        "dab_libraries":            lib_rows,
        "dab_workspace_targets":    target_rows,
    }

    if save:
        repo_dir = os.path.join(output_dir, repo)
        for key, rows in results.items():
            _save(key, rows, repo, repo_dir)

    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Corteva MIC — Azure DevOps DAB scanner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--org",        required=True,
                        help="Azure DevOps org name (e.g. vs-pioneer)")
    parser.add_argument("--project",    required=True,
                        help="Azure DevOps project name (e.g. project0)")
    parser.add_argument("--repo",       default="",
                        help="Single repo name to scan. Omit to scan all repos in the project.")
    parser.add_argument("--token",      default=os.environ.get("ADO_TOKEN", ""),
                        help="Azure DevOps PAT token (or set ADO_TOKEN env var)")
    parser.add_argument("--save",       action="store_true",
                        help="Write JSON + CSV files to disk")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR,
                        help=f"Root output directory (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--json",       action="store_true",
                        help="Print full JSON to stdout")
    args = parser.parse_args()

    if not args.token:
        parser.error("--token is required (or set the ADO_TOKEN environment variable)")

    ado = ADOClient(org=args.org, project=args.project, token=args.token)

    if args.repo:
        repos = [args.repo]
    else:
        print("  Listing repos...", file=sys.stderr)
        repos = ado.list_repos()
        print(f"  Found {len(repos)} repo(s): {', '.join(repos)}", file=sys.stderr)

    all_results: dict[str, dict] = {}
    for repo in repos:
        all_results[repo] = scan_repo(ado, repo, args.output_dir, save=args.save)

    if args.json:
        print(json.dumps(all_results, indent=2, default=str))
    elif not args.save:
        for repo, results in all_results.items():
            for key, rows in results.items():
                if not rows:
                    continue
                print(f"\n{'=' * 72}")
                print(f"  {repo} / {key}  ({len(rows)} rows)")
                print("=" * 72)
                for row in rows:
                    line = "  |  ".join(f"{k}: {v}" for k, v in row.items() if v)
                    print(f"  {line}")


if __name__ == "__main__":
    main()
