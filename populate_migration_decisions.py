#!/usr/bin/env python3
"""
Corteva MIC — Migration Decision Auto-Populator
================================================
Reads the client's manually-pulled inventory Excel files and pre-fills the
migration_decision + notes columns in the tracker workbooks.

Sources (clients_manually_pulled_inventory folder):
  UNITY_CATALOG_TABLE_ASSETS.xlsx  → tables sheet
  DABS_TRACKER.xlsx                → pipelines + jobs sheets

Coverage mapping (from client labels):
  SEED  → move        (new tenant)
  CROP  → stay        (current tenant)
  CP    → stay        (Crop Protection, synonym for CROP)
  BOTH  → both        (keep in current + migrate to new)
  TC    → stay        (internal grouping — stays)
  SPIN_CO cleared=YES → move
  SPIN_CO migrate=YES → move
  DELETE_ALL schemas  → deprecate (all tables in that schema)
  DAB Active=NO       → deprecate

Lookup priority:
  1. (catalog, schema, table) — exact match when source file had a Catalog column
  2. (schema, table)          — broad match for sheets without a Catalog column
  3. schema in DELETE_ALL set → deprecate (overrides per-table coverage)
  Already-set cells are never overwritten.

─── Usage ───────────────────────────────────────────────────────────────────
    python populate_migration_decisions.py
    python populate_migration_decisions.py --client-dir /path/to/client_folder
    python populate_migration_decisions.py --tracker-dir /path/to/tracker_folder
"""

from __future__ import annotations

import argparse
import glob
import os
import sys

try:
    from openpyxl import load_workbook
except ImportError:
    print("ERROR: openpyxl is required.  pip install openpyxl", file=sys.stderr)
    sys.exit(1)


DEFAULT_CLIENT_DIR = os.path.expanduser(
    "~/corteva-mic-workspace-assets-client-version/"
    "clients_manually_pulled_inventory/"
    "Environment Resources_UC Table Migration_DABS TRACKER Info EXCEL"
)
DEFAULT_TRACKER_DIR = os.path.expanduser(
    "~/corteva-mic-workspace-assets-client-version/workspace_inventory_sdk"
)

COVERAGE_MAP: dict[str, str] = {
    "seed": "move",
    "crop": "stay",
    "cp":   "stay",
    "both": "both",
    "tc":   "stay",
}


# ─── helpers ─────────────────────────────────────────────────────────────────

def _cov(value) -> str | None:
    if not value:
        return None
    return COVERAGE_MAP.get(str(value).strip().lower())


def _norm(value) -> str:
    return str(value).strip().lower() if value else ""


def _header_map(ws, header_row=1) -> dict[str, int]:
    """Return {column_name: 1-based column index}, first occurrence wins."""
    result: dict[str, int] = {}
    for cell in ws[header_row]:
        if cell.value and str(cell.value).strip() not in result:
            result[str(cell.value).strip()] = cell.column
    return result


# ─── load client decisions ────────────────────────────────────────────────────

def build_table_decisions(
    client_dir: str,
) -> tuple[dict, dict, set[str]]:
    """
    Returns:
        cat_decisions : {(catalog_lc, schema_lc, table_lc): {"decision", "notes"}}
                        — populated for sheets that have an explicit Catalog column
        decisions     : {(schema_lc, table_lc): {"decision", "notes"}}
                        — populated for sheets without a Catalog column (broad match)
        deprecate_schemas : {schema_lc}  — entire schema → deprecate
    """
    path = os.path.join(client_dir, "UNITY_CATALOG_TABLE_ASSETS.xlsx")
    wb = load_workbook(path, read_only=True, data_only=True)

    cat_decisions: dict[tuple, dict] = {}
    decisions: dict[tuple, dict]     = {}
    deprecate_schemas: set[str]       = set()

    def _put_with_cat(catalog, schema, table, decision, notes=None):
        if not schema or not table:
            return
        k = (_norm(catalog), _norm(schema), _norm(table))
        if k not in cat_decisions:
            cat_decisions[k] = {"decision": decision, "notes": str(notes) if notes else ""}

    def _put(schema, table, decision, notes=None):
        if not schema or not table:
            return
        k = (_norm(schema), _norm(table))
        if k not in decisions:
            decisions[k] = {"decision": decision, "notes": str(notes) if notes else ""}

    # NEW_CORTEVA_SOURCE  — Catalog | schema_name | table_name | Coverage | ... | Notes
    ws = wb["NEW_CORTEVA_SOURCE"]
    h = _header_map(ws)
    for row in ws.iter_rows(min_row=2, values_only=True):
        _put_with_cat(
            row[h["Catalog"]     - 1] if "Catalog"     in h else None,
            row[h["schema_name"] - 1],
            row[h["table_name"]  - 1],
            _cov(row[h["Coverage"] - 1]),
            row[h["Notes"] - 1] if "Notes" in h else None,
        )

    # NEW_CORTEVA_GOLD  — schema_name | table_name | Coverage | ... | Notes  (no Catalog)
    ws = wb["NEW_CORTEVA_GOLD"]
    h = _header_map(ws)
    for row in ws.iter_rows(min_row=2, values_only=True):
        _put(
            row[h["schema_name"] - 1],
            row[h["table_name"]  - 1],
            _cov(row[h["Coverage"] - 1]),
            row[h["Notes"] - 1] if "Notes" in h else None,
        )

    # NEW_CORTEVA_STAR_SCHEMAS  — Schema | Table | Coverage | ... | NOTES  (no Catalog)
    ws = wb["NEW_CORTEVA_STAR_SCHEMAS"]
    h = _header_map(ws)
    notes_col = h.get("NOTES") or h.get("Notes")
    for row in ws.iter_rows(min_row=2, values_only=True):
        _put(
            row[h["Schema"] - 1],
            row[h["Table"]  - 1],
            _cov(row[h["Coverage"] - 1]),
            row[notes_col - 1] if notes_col else None,
        )

    # SPIN_CO_SOURCE  — Schema | Source Schema | Table | Cleared or Not Cleared  (no Catalog)
    ws = wb["SPIN_CO_SOURCE"]
    h = _header_map(ws)
    cleared_col = h.get("Cleared or Not Cleared")
    for row in ws.iter_rows(min_row=2, values_only=True):
        schema  = row[h["Schema"] - 1]
        table   = row[h["Table"]  - 1]
        cleared = row[cleared_col - 1] if cleared_col else None
        if _norm(cleared) == "yes":
            _put(schema, table, "move")

    # SPIN_CO_GOLD  — Schema | Table | Coverage | Migrate  (no Catalog)
    ws = wb["SPIN_CO_GOLD"]
    h = _header_map(ws)
    migrate_col = h.get("Migrate")
    for row in ws.iter_rows(min_row=2, values_only=True):
        schema  = row[h["Schema"]  - 1]
        table   = row[h["Table"]   - 1]
        migrate = row[migrate_col  - 1] if migrate_col else None
        cov     = row[h["Coverage"]- 1] if "Coverage" in h else None
        if _norm(migrate) == "yes":
            _put(schema, table, "move")
        elif cov:
            _put(schema, table, _cov(cov))

    # DELETE_ALL_AND_DON'T_MIGRATE  — Schema  (schema-level deprecation)
    ws = wb["DELETE_ALL_AND_DON'T_MIGRATE"]
    for row in ws.iter_rows(min_row=2, values_only=True):
        schema = row[0]
        if schema:
            deprecate_schemas.add(_norm(schema))

    # FARMER_MASTER  — Catalog | schema_name | table_name | Coverage
    ws = wb["FARMER_MASTER"]
    h = _header_map(ws)
    for row in ws.iter_rows(min_row=2, values_only=True):
        _put_with_cat(
            row[h["Catalog"]     - 1] if "Catalog"     in h else None,
            row[h["schema_name"] - 1],
            row[h["table_name"]  - 1],
            _cov(row[h["Coverage"] - 1]),
        )

    wb.close()
    return cat_decisions, decisions, deprecate_schemas


def build_dab_decisions(client_dir: str) -> dict[str, dict]:
    """
    Returns {bundle_key: {"decision": str|None, "notes": str}}
    Only Active=NO entries carry decision="deprecate"; YES → no decision set here.
    """
    path = os.path.join(client_dir, "DABS_TRACKER.xlsx")
    wb = load_workbook(path, read_only=True, data_only=True)
    ws = wb["DABs Inventory"]

    dab: dict[str, dict] = {}
    h = _header_map(ws)
    for row in ws.iter_rows(min_row=2, values_only=True):
        bundle = row[h["Bundle Name"] - 1] if "Bundle Name" in h else row[0]
        active = row[h["Active"] - 1]      if "Active" in h       else row[7]
        notes  = row[h["Notes"]  - 1]      if "Notes"  in h       else row[8]
        if not bundle:
            continue
        key = _norm(bundle)
        if key.endswith(".yml"):
            key = key[:-4]
        decision = "deprecate" if _norm(active) == "no" else None
        if key not in dab:   # first occurrence wins (some bundles appear multiple times)
            dab[key] = {"decision": decision, "notes": str(notes) if notes else ""}

    wb.close()
    return dab


# ─── apply decisions to a tracker workbook ───────────────────────────────────

def _update_sheet(ws, cat_decisions, decisions, deprecate_schemas, dab_decisions) -> int:
    h = _header_map(ws)
    dec_col   = h.get("migration_decision")
    notes_col = h.get("notes")
    if not dec_col:
        return 0

    has_catalog = "catalog_name" in h
    has_schema  = "schema_name"  in h
    has_table   = "table_name"   in h
    has_name    = "name"         in h

    updated = 0
    for row in ws.iter_rows(min_row=2):
        dec_cell = row[dec_col - 1]
        if dec_cell.value:
            continue

        decision = notes_txt = None

        if has_schema and has_table:
            catalog   = row[h["catalog_name"] - 1].value if has_catalog else None
            schema    = row[h["schema_name"]  - 1].value
            table     = row[h["table_name"]   - 1].value
            schema_lc = _norm(schema)
            table_lc  = _norm(table)

            if schema_lc in deprecate_schemas:
                decision = "deprecate"
            else:
                # Try catalog-specific match first, then broad schema+table match
                if catalog:
                    hit = cat_decisions.get((_norm(catalog), schema_lc, table_lc))
                    if hit:
                        decision  = hit["decision"]
                        notes_txt = hit["notes"]
                if not decision:
                    hit = decisions.get((schema_lc, table_lc))
                    if hit:
                        decision  = hit["decision"]
                        notes_txt = hit["notes"]

        elif has_name:
            name = row[h["name"] - 1].value
            if name:
                hit = dab_decisions.get(_norm(name))
                if hit:
                    decision  = hit["decision"]
                    notes_txt = hit["notes"]

        if decision:
            dec_cell.value = decision
            if notes_col and notes_txt:
                row[notes_col - 1].value = notes_txt
            updated += 1

    return updated


def populate_tracker(
    tracker_path: str,
    cat_decisions: dict,
    decisions: dict,
    deprecate_schemas: set,
    dab_decisions: dict,
) -> int:
    print(f"  {os.path.basename(tracker_path)}", file=sys.stderr)
    wb = load_workbook(tracker_path)
    total = 0

    for sheet_name in ("tables", "pipelines", "jobs"):
        if sheet_name not in wb.sheetnames:
            continue
        n = _update_sheet(
            wb[sheet_name],
            cat_decisions, decisions, deprecate_schemas, dab_decisions,
        )
        print(f"    {sheet_name:<12} {n:>5} cells updated", file=sys.stderr)
        total += n

    wb.save(tracker_path)
    print(f"  -> saved  ({total} total updates)\n", file=sys.stderr)
    return total


# ─── main ────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Auto-populate migration_decision in tracker Excel files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--client-dir", default=DEFAULT_CLIENT_DIR,
        help=f"Folder with client inventory Excel files (default: {DEFAULT_CLIENT_DIR})",
    )
    parser.add_argument(
        "--tracker-dir", default=DEFAULT_TRACKER_DIR,
        help=f"Folder with tracker_*.xlsx files (default: {DEFAULT_TRACKER_DIR})",
    )
    args = parser.parse_args()

    if not os.path.isdir(args.client_dir):
        print(f"ERROR: client-dir not found:\n  {args.client_dir}", file=sys.stderr)
        sys.exit(1)

    print("\n  Loading client inventory ...", file=sys.stderr)
    cat_decisions, decisions, deprecate_schemas = build_table_decisions(args.client_dir)
    dab_decisions = build_dab_decisions(args.client_dir)
    print(f"    catalog-specific decisions : {len(cat_decisions)}", file=sys.stderr)
    print(f"    broad schema+table decisions: {len(decisions)}", file=sys.stderr)
    print(f"    deprecate schemas           : {len(deprecate_schemas)}", file=sys.stderr)
    print(f"    DAB decisions               : {len(dab_decisions)}", file=sys.stderr)

    trackers = sorted(glob.glob(os.path.join(args.tracker_dir, "tracker_*.xlsx")))
    if not trackers:
        print(f"ERROR: no tracker_*.xlsx found in {args.tracker_dir}", file=sys.stderr)
        sys.exit(1)

    print(f"\n  Updating {len(trackers)} tracker(s) ...\n", file=sys.stderr)
    for tracker_path in trackers:
        populate_tracker(tracker_path, cat_decisions, decisions, deprecate_schemas, dab_decisions)

    print("  Done.", file=sys.stderr)


if __name__ == "__main__":
    main()
