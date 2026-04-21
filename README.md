# Corteva MIC — Databricks Workspace Inventory

A reusable tool to inventory assets across one or multiple Databricks workspaces.

There are two versions of the inventory script — choose based on your environment:

| | `inventory.py` (v1) | `inventory_v2.py` (v2) |
|---|---|---|
| **Approach** | Direct REST API calls via Python built-in `urllib` | Databricks Python SDK (`databricks-sdk`) |
| **Dependencies** | None — Python standard library only | `pip install databricks-sdk` |
| **Auth** | PAT token only | PAT token, `~/.databrickscfg` profiles, env vars, OAuth |
| **Best for** | Environments where pip install is unavailable | Environments where the SDK can be installed |

---

## What it collects

| Asset | Details |
|---|---|
| **Jobs** | Name, creator, schedule, last run time & status |
| **DLT Pipelines** | Name, creator, trigger type, last run time |
| **Notebooks** | Path, language |
| **Tables** | Catalog, schema, table name, type, format, owner, created/updated timestamps |
| **Volumes** | Catalog, schema, volume name, type, storage location, owner |
| **Functions** | Catalog, schema, function name, language, owner |
| **Genie Spaces** | Space ID, title |
| **ML Experiments** | Name, lifecycle stage, artifact location |
| **Dashboards** | Name, type (Lakeview or Classic) |
| **Serving Endpoints** | Name, creator, ready state (covers agents & models) |
| **Apps** | Name, description, status, URL |
| **Repos / Git Folders** | Path, Git URL, provider, branch, owner |
| **Registered ML Models** | Full name, catalog, schema, owner (Unity Catalog + legacy registry) |

Output per asset type: one `.json` file + one `.csv` file, saved under `output/<workspace-name>/`.

---

## Requirements

### v1 — `inventory.py`

- Python 3.9+
- No external packages — `urllib`, `csv`, and `json` are all part of the Python standard library

### v2 — `inventory_v2.py`

- Python 3.9+
- `databricks-sdk >= 0.20.0`

Install via Databricks internal PyPI proxy:
```bash
pip3 install databricks-sdk --index-url https://pypi-proxy.dev.databricks.com/simple
```

Install via public PyPI (if accessible):
```bash
pip3 install databricks-sdk
```

---

## Setup

### 1. Clone or download this folder

```bash
git clone <repo-url>
cd corteva-mic-inventory-pull-reusable
```

### 2. Configure your workspaces

Copy `workspaces.template.json` to `workspaces.json` and fill in credentials for each workspace.

**v1 — PAT token only:**
```json
[
  {
    "name": "sales-mi-dbw-01-dev",
    "host": "https://adb-4225902524755119.19.azuredatabricks.net",
    "token": "dapi..."
  }
]
```

**v2 — supports PAT token or `~/.databrickscfg` profile:**
```json
[
  {
    "name": "sales-mi-dbw-01-dev",
    "host": "https://adb-4225902524755119.19.azuredatabricks.net",
    "token": "dapi..."
  },
  {
    "name": "sales-mi-dbw-01-uat",
    "host": "https://adb-8347049335921990.10.azuredatabricks.net",
    "profile": "my-uat-profile"
  }
]
```

**How to generate a PAT token:**
1. Log into your Databricks workspace
2. Click your profile icon (top right) → **Settings**
3. Left sidebar → **Developer** → **Access tokens**
4. Click **Generate new token**, give it a name and expiry
5. Copy the `dapi...` token and paste it into `workspaces.json`

> ⚠️ `workspaces.json` is gitignored — your tokens will never be committed to Git.

---

## Usage

### v1 — `inventory.py` (no dependencies)

```bash
# All workspaces from config
python3 inventory.py --config workspaces.json

# Single workspace
python3 inventory.py --host https://adb-xxx.azuredatabricks.net --token dapi...

# One asset type only
python3 inventory.py --config workspaces.json --section tables

# Save to a custom directory
python3 inventory.py --config workspaces.json --output-dir /path/to/output

# Print JSON to stdout
python3 inventory.py --host https://... --token dapi... --json > out.json
```

### v2 — `inventory_v2.py` (requires databricks-sdk)

```bash
# All workspaces from config
python3 inventory_v2.py --config workspaces.json

# Single workspace — PAT token
python3 inventory_v2.py --host https://adb-xxx.azuredatabricks.net --token dapi...

# Single workspace — ~/.databrickscfg profile
python3 inventory_v2.py --profile my-profile

# One asset type only
python3 inventory_v2.py --profile my-profile --section jobs

# Save to a custom directory
python3 inventory_v2.py --config workspaces.json --output-dir /path/to/output

# Print JSON to stdout
python3 inventory_v2.py --profile my-profile --json > out.json
```

---

## Output structure

```
output/
├── sales-mi-dbw-01-dev/
│   ├── sales-mi-dbw-01-dev_jobs.csv
│   ├── sales-mi-dbw-01-dev_jobs.json
│   ├── sales-mi-dbw-01-dev_pipelines.csv
│   ├── sales-mi-dbw-01-dev_pipelines.json
│   ├── sales-mi-dbw-01-dev_tables.csv
│   └── ... (one pair per asset type)
├── sales-mi-dbw-01-prod/
│   └── ...
└── mic-databricks-dev/
    └── ...
```

---

## Available sections

Pass any of these to `--section` to collect only that asset type:

`jobs` · `pipelines` · `notebooks` · `tables` · `volumes` · `functions` · `genie_spaces` · `experiments` · `dashboards` · `serving_endpoints` · `apps` · `repos` · `registered_models`

---

## Permissions

The script collects whatever the provided token has access to. Results may be partial for tokens with limited permissions — the script will flag any permission-denied endpoints in the run summary rather than failing silently.

For a complete inventory, the token should belong to a **workspace admin** or a service principal with broad read access.

---

## Workspaces

| Workspace | Host |
|---|---|
| sales-mi-dbw-01-dev | https://adb-4225902524755119.19.azuredatabricks.net |
| sales-mi-dbw-01-uat | https://adb-8347049335921990.10.azuredatabricks.net |
| sales-mi-dbw-01-prod | https://adb-2281982956507820.0.azuredatabricks.net |
| mic-databricks-dev | https://adb-7405607360771421.1.azuredatabricks.net |
| mic-databricks-uat | https://adb-7405604679876710.10.azuredatabricks.net |
| mic-databricks-prod | https://adb-7405607553229575.15.azuredatabricks.net |
| mic-databricks-lab | https://adb-7405619576373019.19.azuredatabricks.net |
