# Corteva MIC — Databricks Workspace Inventory

A reusable script to inventory assets across one or multiple Databricks workspaces. No external dependencies — runs with standard Python 3.

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

- Python 3.9+
- No external packages needed

---

## Setup

### 1. Clone or download this folder

```bash
git clone <repo-url>
cd corteva-mic-inventory-pull-reusable
```

### 2. Configure your workspaces

Open `workspaces.json` and fill in your PAT token for each workspace you want to inventory. Leave the token blank to skip a workspace.

```json
[
  {
    "name": "sales-mi-dbw-01-dev",
    "host": "https://adb-4225902524755119.19.azuredatabricks.net",
    "token": "dapi..."
  },
  {
    "name": "sales-mi-dbw-01-prod",
    "host": "https://adb-2281982956507820.0.azuredatabricks.net",
    "token": ""
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

### Run across all configured workspaces

```bash
python3 inventory.py --config workspaces.json
```

### Run for a single workspace

```bash
python3 inventory.py \
  --host https://adb-xxx.azuredatabricks.net \
  --token dapi...
```

### Collect one asset type only

```bash
python3 inventory.py --config workspaces.json --section tables
```

### Save output to a custom directory

```bash
python3 inventory.py --config workspaces.json --output-dir /path/to/my/output
```

### Print JSON to stdout (single workspace)

```bash
python3 inventory.py --host https://... --token dapi... --json > out.json
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
