# Corteva MIC — Databricks Workspace and UC Inventory

A reusable tool to inventory assets across one or multiple Databricks workspaces.

There are two scripts — choose based on the environment you are running against:

| | `inventory_pull_urllib.py` | `inventory_pull_sdk.py` |
|---|---|---|
| **Approach** | Direct REST API calls via Python built-in `urllib` | Databricks Python SDK (`databricks-sdk`) |
| **Dependencies** | None — Python standard library only | `pip install databricks-sdk` |
| **Auth** | PAT token only | PAT token, `~/.databrickscfg` profiles, env vars, OAuth M2M |
| **Recommended for** | Dev and UAT — PAT tokens are available in lower environments | Production — service principals with OAuth M2M are required |

**Why the distinction?**
In **Dev and UAT** workspaces, users can typically generate PAT tokens directly in the workspace settings. `inventory_pull_urllib.py` requires no installation and works immediately with a PAT token.

In **Production** workspaces, PAT tokens are often disabled for security reasons. Access must go through a service principal using OAuth M2M (machine-to-machine) authentication. `inventory_pull_sdk.py` handles this natively via the Databricks SDK — no manual token management needed.

---

## Quick Start

Follow these steps to go from zero to a full inventory.

### Step 1 — Clone the repo

```bash
git clone https://github.com/dariakrasavina/corteva-mic-inventory-pull.git
cd corteva-mic-inventory-pull
```

### Step 2 — Install the Databricks SDK

```bash
pip3 install databricks-sdk --index-url https://pypi-proxy.dev.databricks.com/simple
```

> If you have access to public PyPI: `pip3 install databricks-sdk`

### Step 3 — Authenticate to your workspace

Run this once per workspace you want to inventory. Replace `<workspace-host>` with your workspace URL and `<profile-name>` with any name you choose (e.g. `dev`, `uat`, `prod`):

```bash
databricks auth login --host <workspace-host> --profile <profile-name>
```

A browser window will open — log in with your Databricks account. The token is saved automatically to `~/.databrickscfg`.

Repeat for each workspace, using a different profile name each time:

```bash
databricks auth login --host <workspace-host-1> --profile <profile-name-1>
databricks auth login --host <workspace-host-2> --profile <profile-name-2>
databricks auth login --host <workspace-host-3> --profile <profile-name-3>
```

> Workspace host URLs can be found in the [Workspaces](#workspaces) table at the bottom of this README.

### Step 4 — Run the inventory

```bash
python3 inventory_pull_sdk.py --profile <profile-name> --save
```

Output is saved to `~/corteva-mic-workspace-assets/output/<profile-name>/` — one JSON and one CSV file per asset type.

To run against multiple workspaces at once, fill in `workspaces.json` with the profile for each workspace (see [Setup](#setup)) and run:

```bash
python3 inventory_pull_sdk.py --config workspaces.json
```

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

### `inventory_pull_urllib.py`

- Python 3.9+
- No external packages — `urllib`, `csv`, and `json` are all part of the Python standard library

### `inventory_pull_sdk.py`

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

## Authentication

Both scripts need credentials to connect to each Databricks workspace. The method you use depends on which script you are running and what your workspace allows.

### Available authentication methods

| Method | Supported by | How it works |
|---|---|---|
| **PAT token** | Both scripts | A `dapi...` token generated in your Databricks workspace settings |
| **`~/.databrickscfg` profile** | `inventory_pull_sdk.py` only | A named profile set up by the Databricks CLI (`databricks configure`) |
| **Environment variables** | `inventory_pull_sdk.py` only | `DATABRICKS_HOST` + `DATABRICKS_TOKEN` set in your shell |
| **OAuth / Azure AD** | `inventory_pull_sdk.py` only | Handled automatically by the SDK if configured |

> If you are using **`inventory_pull_urllib.py`**, PAT token is your only option.
> If you are using **`inventory_pull_sdk.py`**, any of the above methods work.

### How to generate a PAT token

If your workspace allows PAT tokens (not all do — check with your workspace admin):

1. Log into your Databricks workspace
2. Click your profile icon (top right) → **Settings**
3. Left sidebar → **Developer** → **Access tokens**
4. Click **Generate new token**, give it a name and expiry
5. Copy the `dapi...` token — it is only shown once

> ⚠️ Never commit your token to Git. `workspaces.json` is gitignored for this reason.

### How to set up a `~/.databrickscfg` profile (`inventory_pull_sdk.py` only)

If your workspace does not allow PAT tokens, use the Databricks CLI to configure a profile:

```bash
databricks configure --profile my-profile
```

You will be prompted for the workspace host and your credentials. Once set up, pass `--profile my-profile` to `inventory_pull_sdk.py`.

---

## Setup

### 1. Clone or download this folder

```bash
git clone <repo-url>
cd corteva-mic-inventory-pull-reusable
```

### 2. Configure your workspaces

Copy `workspaces.template.json` to `workspaces.json` and fill in credentials for each workspace.

**`inventory_pull_urllib.py` — PAT token only:**
```json
[
  {
    "name": "sales-mi-dbw-01-dev",
    "host": "https://adb-4225902524755119.19.azuredatabricks.net",
    "token": "dapi..."
  }
]
```

**`inventory_pull_sdk.py` — supports PAT token or `~/.databrickscfg` profile:**
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

> ⚠️ `workspaces.json` is gitignored — your tokens will never be committed to Git.

---

## Usage

### `inventory_pull_urllib.py` (no dependencies)

```bash
# All workspaces from config
python3 inventory_pull_urllib.py --config workspaces.json

# Single workspace
python3 inventory_pull_urllib.py --host https://adb-xxx.azuredatabricks.net --token dapi...

# One asset type only
python3 inventory_pull_urllib.py --config workspaces.json --section tables

# Save to a custom directory
python3 inventory_pull_urllib.py --config workspaces.json --output-dir /path/to/output

# Print JSON to stdout
python3 inventory_pull_urllib.py --host https://... --token dapi... --json > out.json
```

### `inventory_pull_sdk.py` (requires databricks-sdk)

```bash
# All workspaces from config
python3 inventory_pull_sdk.py --config workspaces.json

# Single workspace — PAT token
python3 inventory_pull_sdk.py --host https://adb-xxx.azuredatabricks.net --token dapi...

# Single workspace — ~/.databrickscfg profile
python3 inventory_pull_sdk.py --profile my-profile

# One asset type only
python3 inventory_pull_sdk.py --profile my-profile --section jobs

# Save to a custom directory
python3 inventory_pull_sdk.py --config workspaces.json --output-dir /path/to/output

# Print JSON to stdout
python3 inventory_pull_sdk.py --profile my-profile --json > out.json
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

## Service Principals (production use)

Service principals are the recommended way to run this script in production — they don't expire like PAT tokens, can be managed centrally, and work in workspaces where PAT tokens are disabled.

**`inventory_pull_urllib.py`** does not support service principals — it only accepts PAT tokens. Use this script for Dev and UAT environments where PAT tokens are available.

**`inventory_pull_sdk.py`** fully supports service principals via OAuth M2M (machine-to-machine). Use this script for production environments where PAT tokens are disabled and access must go through a service principal.

### Option A — environment variables

```bash
DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net \
DATABRICKS_CLIENT_ID=<sp-client-id> \
DATABRICKS_CLIENT_SECRET=<sp-secret> \
python3 inventory_pull_sdk.py
```

### Option B — `~/.databrickscfg` profile

Add a named profile for the service principal:

```ini
[prod-sp]
host          = https://adb-xxx.azuredatabricks.net
client_id     = <sp-client-id>
client_secret = <sp-secret>
```

Then run:

```bash
python3 inventory_pull_sdk.py --profile prod-sp
```

### Permissions required

The service principal needs the following access on the target workspace:

| Resource | Required permission |
|---|---|
| Jobs, pipelines, notebooks, repos, apps | Workspace read access (CAN VIEW) |
| Tables, volumes, functions, models | `USE CATALOG` + `USE SCHEMA` on each catalog/schema |
| Serving endpoints, experiments, dashboards | Workspace read access |
| Full inventory | Workspace admin or broad read-only service principal |

---

## Permissions

The script collects whatever the provided credentials have access to. Results may be partial for tokens or service principals with limited permissions — the script will flag any permission-denied endpoints in the run summary rather than failing silently.

For a complete inventory, the credentials should belong to a **workspace admin** or a service principal with broad read access.

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
