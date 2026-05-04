#!/usr/bin/env python3
"""
Corteva MIC — Migration Tracker Generator (workspace inventory)
===============================================================
Reads workspace_inventory_sdk output folders for all environments,
combines them into one CSV per asset type, and adds migration decision
columns (migration_decision, notes, owner, priority) for the client to fill in.

No external dependencies — uses Python standard library only.

─── Usage ───────────────────────────────────────────────────────────────────
    python generate_migration_tracker.py
    python generate_migration_tracker.py --input-dir /path/to/workspace_inventory_sdk
    python generate_migration_tracker.py --output-dir /path/to/output/folder
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys

DEFAULT_INPUT  = os.path.expanduser(
    "~/corteva-mic-workspace-assets-client-version/workspace_inventory_sdk"
)
DEFAULT_OUTPUT = os.path.expanduser(
    "~/corteva-mic-workspace-assets-client-version/migration_tracker_workspace_inventory"
)

ASSET_TYPES = [
    "jobs",
    "pipelines",
    "notebooks",
    "tables",
    "volumes",
    "functions",
    "registered_models",
    "serving_endpoints",
    "apps",
    "experiments",
    "dashboards",
    "repos",
    "genie_spaces",
]

DECISION_COLUMNS = [
    "migration_decision",   # stay / move / both / deprecate
    "notes",
    "owner",
    "priority",             # high / medium / low
]


def _read_csv(path: str) -> list[dict]:
    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            with open(path, newline="", encoding=encoding) as f:
                return list(csv.DictReader(f))
        except (UnicodeDecodeError, UnicodeError):
            continue
    return []


def _load_asset_type(input_dir: str, asset_type: str) -> list[dict]:
    """Read CSVs for one asset type across all environment subfolders."""
    rows: list[dict] = []
    env_dirs = sorted(
        d for d in os.listdir(input_dir)
        if os.path.isdir(os.path.join(input_dir, d))
    )
    for env in env_dirs:
        matches = glob.glob(os.path.join(input_dir, env, f"*_{asset_type}.csv"))
        for path in matches:
            for row in _read_csv(path):
                row["workspace"] = env
                rows.append(row)
    return rows


def _write_tracker_csv(rows: list[dict], asset_type: str, output_dir: str) -> str:
    out_path = os.path.join(output_dir, f"tracker_{asset_type}.csv")

    if not rows:
        with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
            f.write("")
        return out_path

    asset_cols = [c for c in rows[0].keys() if c != "workspace"]
    all_cols = ["workspace"] + asset_cols + DECISION_COLUMNS

    with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            for col in DECISION_COLUMNS:
                row[col] = ""
            writer.writerow(row)

    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate migration tracker CSVs from workspace_inventory_sdk output",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input-dir", default=DEFAULT_INPUT,
        help=f"Path to workspace_inventory_sdk output folder (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output-dir", default=DEFAULT_OUTPUT,
        help=f"Output folder for tracker CSVs (default: {DEFAULT_OUTPUT})",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.input_dir):
        print(f"ERROR: input directory not found: {args.input_dir}", file=sys.stderr)
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    print(f"\n  Input  : {args.input_dir}", file=sys.stderr)
    print(f"  Output : {args.output_dir}\n", file=sys.stderr)

    for asset_type in ASSET_TYPES:
        rows = _load_asset_type(args.input_dir, asset_type)
        out_path = _write_tracker_csv(rows, asset_type, args.output_dir)
        print(f"  {asset_type:<25} {len(rows):>5} rows  ->  {os.path.basename(out_path)}", file=sys.stderr)

    print(f"\n  Done. Open any tracker_*.csv in Excel to start filling in migration decisions.", file=sys.stderr)


if __name__ == "__main__":
    main()
