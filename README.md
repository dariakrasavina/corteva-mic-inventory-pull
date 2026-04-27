# Corteva MIC — Databricks Workspace and UC Inventory

A reusable tool to inventory assets across one or multiple Databricks workspaces.

## Scripts

### Asset inventory — choose one based on your environment

Both scripts collect the same workspace assets (jobs, tables, notebooks, models, and more). The difference is the approach and what authentication methods they support:

| | `workspace_inventory_api.py` | `workspace_inventory_sdk.py` |
|---|---|---|
| **Approach** | Direct REST API calls via Python built-in `urllib` | Databricks Python SDK (`databricks-sdk`) |
| **Dependencies** | None — Python standard library only | `pip install databricks-sdk` |
| **Auth** | PAT token only | PAT token, OAuth U2M (browser login), OAuth M2M (service principal), env vars |
| **Recommended for** | Dev and UAT — PAT tokens are available in lower environments | Production — service principals with OAuth M2M are required |

In **Dev and UAT** workspaces, users can typically generate PAT tokens directly in the workspace settings. `workspace_inventory_api.py` requires no installation and works immediately with a PAT token.

In **Production** workspaces, PAT tokens are often disabled for security reasons. Access must go through a service principal using OAuth M2M (machine-to-machine) authentication. `workspace_inventory_sdk.py` handles this natively via the Databricks SDK — no manual token management needed.

---

### Configuration inventory — run alongside the asset inventory

`workspace_config_inventory_sdk.py` is a **separate, complementary script**. It does not collect data assets — it collects workspace-level **configuration**. Run it alongside either asset inventory script to get a complete picture of a workspace before a migration.

It covers:

- **Identity** — users, groups, service principals
- **Compute** — clusters, cluster policies, instance pools, SQL warehouses
- **Unity Catalog** — external locations, storage credentials, connections (Lakehouse Federation)
- **Workspace Settings** — all three settings APIs: legacy workspace_conf (37 known keys), V2 feature flags (100+ settings), and typed settings
- **Platform Resources** — SQL global config, global init scripts, IP access lists, secret scopes (names and key names only — values are never read), managed PAT tokens
- **SQL Assets** — saved SQL queries, SQL alerts

---

## Quick Start

Before you run the script, answer one question: **are you a human running this manually, or is this being run automatically by a service principal?**

The reason this matters: a human can authenticate by logging in through a browser. A service principal is an automated account with no ability to open a browser — it must authenticate using a pre-configured client ID and secret instead.

---

### If you are a human user (running manually on Dev or UAT)

**Step 1 — Clone the repo**

```bash
git clone https://github.com/dariakrasavina/corteva-mic-inventory-pull.git
cd corteva-mic-inventory-pull
```

**Step 2 — Choose which script to use**

This determines how you authenticate and whether you need to install anything:

| | `workspace_inventory_api.py` | `workspace_inventory_sdk.py` |
|---|---|---|
| **Requires install** | No — works out of the box | Yes — must install Databricks SDK first |
| **Auth method** | PAT token (generated in workspace settings) | OAuth U2M — log in via browser, no token needed |
| **When to use** | Quickest option if you can generate a PAT token | Use if your workspace does not allow PAT tokens |

---

**If using `workspace_inventory_api.py` (PAT token — no install needed):**

Generate a PAT token in your workspace settings (see [How to generate a PAT token](#how-to-generate-a-pat-token)), then run:

```bash
python3 workspace_inventory_api.py --host <workspace-host> --token <your-pat-token> --save
```

---

**If using `workspace_inventory_sdk.py` (OAuth U2M — browser login):**

First install the Databricks SDK:

```bash
pip3 install databricks-sdk --index-url https://pypi-proxy.dev.databricks.com/simple
```

Then log in to your workspace via browser. This command opens a browser window where you sign in with your Databricks account. Run it once per workspace — you can use any name you like for `<profile-name>` (e.g. `dev`, `uat`):

```bash
databricks auth login --host <workspace-host> --profile <profile-name>
```

After logging in, your credentials are saved automatically to `~/.databrickscfg`. You will not need to log in again until the token expires.

Then run the asset inventory:

```bash
python3 workspace_inventory_sdk.py --profile <profile-name> --save
```

And optionally run the configuration inventory using the same profile:

```bash
python3 workspace_config_inventory_sdk.py --profile <profile-name> --save
```

> Workspace host URLs can be found in the [Workspaces](#workspaces) table at the bottom of this README.

Output files are saved to `~/corteva-mic-workspace-assets/output/<profile-name>/`.

---

### If you are a service principal (running automatically on Production)

A service principal cannot log in through a browser. Instead, it uses a **client ID and client secret** that are pre-configured before the script runs. These credentials are created by a workspace admin in the Databricks account console under **Service Principals**.

There are two ways to provide the credentials:

**Option 1 — Environment variables (recommended for automated pipelines)**

Set these three environment variables before running the script. The SDK will pick them up automatically:

```bash
export DATABRICKS_HOST=<workspace-host>
export DATABRICKS_CLIENT_ID=<sp-client-id>
export DATABRICKS_CLIENT_SECRET=<sp-client-secret>

python3 workspace_inventory_sdk.py --save
python3 workspace_config_inventory_sdk.py --save
```

**Option 2 — Config file (`~/.databrickscfg`)**

If you prefer a config file, manually add a profile for the service principal. Unlike the human login flow, this does not require a browser — you just paste the credentials directly into the file:

```ini
[<profile-name>]
host          = <workspace-host>
client_id     = <sp-client-id>
client_secret = <sp-client-secret>
```

Then run:

```bash
python3 workspace_inventory_sdk.py --profile <profile-name> --save
python3 workspace_config_inventory_sdk.py --profile <profile-name> --save
```

Output files are saved to `~/corteva-mic-workspace-assets/output/<profile-name>/`.

---

### Running against multiple workspaces at once

Regardless of whether you are a human or service principal, you can run the inventory across all workspaces in one go. Fill in `workspaces.json` with a profile name for each workspace (see [Setup](#setup)) and run:

```bash
python3 workspace_inventory_sdk.py --config workspaces.json
python3 workspace_config_inventory_sdk.py --config workspaces.json
```

---

## What it collects

### `workspace_inventory_api.py` and `workspace_inventory_sdk.py` — data assets

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

### `workspace_config_inventory_sdk.py` — workspace configuration

| Category | What is collected |
|---|---|
| **Identity** | Users (name, active status), groups (members), service principals |
| **Compute** | Clusters (type, Spark version, autoscale, security mode), cluster policies, instance pools, SQL warehouses |
| **Unity Catalog** | External locations (URL, credential, owner), storage credentials, connections (Lakehouse Federation) |
| **Workspace Settings** | Legacy workspace_conf flags (37 known keys), workspace settings V2 (100+ feature flags), typed settings API, SQL global config |
| **Platform Resources** | Global init scripts (metadata + content), IP access lists, secret scopes (scope names and key names only — values are never read), managed PAT tokens |
| **SQL Assets** | Saved SQL queries, SQL alerts |

> **Security note:** The script lists secret scope names and key names to show what secrets exist. It never reads or exports secret values.

Output per asset type: one `.json` file + one `.csv` file, saved under `output/<workspace-name>/`.

---

## Requirements

### `workspace_inventory_api.py`

- Python 3.9+
- No external packages — `urllib`, `csv`, and `json` are all part of the Python standard library

### `workspace_inventory_sdk.py` and `workspace_config_inventory_sdk.py`

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
| **PAT token** | All three scripts | A `dapi...` token generated in your Databricks workspace settings |
| **`~/.databrickscfg` profile** | `workspace_inventory_sdk.py` and `workspace_config_inventory_sdk.py` only | A named profile set up by the Databricks CLI (`databricks configure`) |
| **Environment variables** | `workspace_inventory_sdk.py` and `workspace_config_inventory_sdk.py` only | `DATABRICKS_HOST` + `DATABRICKS_TOKEN` set in your shell |
| **OAuth U2M** | `workspace_inventory_sdk.py` and `workspace_config_inventory_sdk.py` only | Browser-based login via `databricks auth login` — no token needed |
| **OAuth M2M (service principal)** | `workspace_inventory_sdk.py` and `workspace_config_inventory_sdk.py` only | `DATABRICKS_HOST` + `DATABRICKS_CLIENT_ID` + `DATABRICKS_CLIENT_SECRET` |

> If you are using **`workspace_inventory_api.py`**, PAT token is your only option.
> If you are using **`workspace_inventory_sdk.py`** or **`workspace_config_inventory_sdk.py`**, any of the above methods work.

### How to generate a PAT token

If your workspace allows PAT tokens (not all do — check with your workspace admin):

1. Log into your Databricks workspace
2. Click your profile icon (top right) → **Settings**
3. Left sidebar → **Developer** → **Access tokens**
4. Click **Generate new token**, give it a name and expiry
5. Copy the `dapi...` token — it is only shown once

> ⚠️ Never commit your token to Git. `workspaces.json` is gitignored for this reason.

### How to set up a `~/.databrickscfg` profile (`workspace_inventory_sdk.py` and `workspace_config_inventory_sdk.py` only)

If your workspace does not allow PAT tokens, use the Databricks CLI to configure a profile:

```bash
databricks configure --profile my-profile
```

You will be prompted for the workspace host and your credentials. Once set up, pass `--profile my-profile` to either SDK-based script.

---

## Setup

### 1. Clone or download this folder

```bash
git clone <repo-url>
cd corteva-mic-inventory-pull-reusable
```

### 2. Configure your workspaces

Copy `workspaces.template.json` to `workspaces.json` and fill in credentials for each workspace.

**`workspace_inventory_api.py` — PAT token only:**
```json
[
  {
    "name": "sales-mi-dbw-01-dev",
    "host": "https://adb-4225902524755119.19.azuredatabricks.net",
    "token": "dapi..."
  }
]
```

**`workspace_inventory_sdk.py` and `workspace_config_inventory_sdk.py` — supports PAT token or `~/.databrickscfg` profile:**
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

### `workspace_inventory_api.py` (no dependencies)

```bash
# All workspaces from config
python3 workspace_inventory_api.py --config workspaces.json

# Single workspace
python3 workspace_inventory_api.py --host https://adb-xxx.azuredatabricks.net --token dapi...

# One asset type only
python3 workspace_inventory_api.py --config workspaces.json --section tables

# Save to a custom directory
python3 workspace_inventory_api.py --config workspaces.json --output-dir /path/to/output

# Print JSON to stdout
python3 workspace_inventory_api.py --host https://... --token dapi... --json > out.json
```

### `workspace_inventory_sdk.py` (requires databricks-sdk)

```bash
# All workspaces from config
python3 workspace_inventory_sdk.py --config workspaces.json

# Single workspace — PAT token
python3 workspace_inventory_sdk.py --host https://adb-xxx.azuredatabricks.net --token dapi...

# Single workspace — ~/.databrickscfg profile
python3 workspace_inventory_sdk.py --profile my-profile

# One asset type only
python3 workspace_inventory_sdk.py --profile my-profile --section jobs

# Save to a custom directory
python3 workspace_inventory_sdk.py --config workspaces.json --output-dir /path/to/output

# Print JSON to stdout
python3 workspace_inventory_sdk.py --profile my-profile --json > out.json
```

### `workspace_config_inventory_sdk.py` (requires databricks-sdk)

Collects workspace configuration rather than data assets. Use alongside `workspace_inventory_sdk.py` for a complete picture.

```bash
# All workspaces from config
python3 workspace_config_inventory_sdk.py --config workspaces.json

# Single workspace — PAT token
python3 workspace_config_inventory_sdk.py --host https://adb-xxx.azuredatabricks.net --token dapi...

# Single workspace — ~/.databrickscfg profile
python3 workspace_config_inventory_sdk.py --profile my-profile

# One section only
python3 workspace_config_inventory_sdk.py --profile my-profile --section users
python3 workspace_config_inventory_sdk.py --profile my-profile --section clusters
python3 workspace_config_inventory_sdk.py --profile my-profile --section workspace_conf_legacy

# Save to a custom directory
python3 workspace_config_inventory_sdk.py --config workspaces.json --output-dir /path/to/output

# Print JSON to stdout
python3 workspace_config_inventory_sdk.py --profile my-profile --json > out.json
```

---

## Output structure

```
output/
├── sales-mi-dbw-01-dev/
│   ├── sales-mi-dbw-01-dev_jobs.csv             ← from workspace_inventory_sdk.py
│   ├── sales-mi-dbw-01-dev_jobs.json
│   ├── sales-mi-dbw-01-dev_tables.csv
│   ├── sales-mi-dbw-01-dev_tables.json
│   ├── sales-mi-dbw-01-dev_users.csv            ← from workspace_config_inventory_sdk.py
│   ├── sales-mi-dbw-01-dev_users.json
│   ├── sales-mi-dbw-01-dev_clusters.csv
│   ├── sales-mi-dbw-01-dev_clusters.json
│   └── ... (one pair per asset/config type)
├── sales-mi-dbw-01-prod/
│   └── ...
└── mic-databricks-dev/
    └── ...
```

---

## Available sections

### `workspace_inventory_api.py` and `workspace_inventory_sdk.py`

Pass any of these to `--section`:

`jobs` · `pipelines` · `notebooks` · `tables` · `volumes` · `functions` · `genie_spaces` · `experiments` · `dashboards` · `serving_endpoints` · `apps` · `repos` · `registered_models`

### `workspace_config_inventory_sdk.py`

Pass any of these to `--section`:

**Identity:** `users` · `groups` · `service_principals`

**Compute:** `clusters` · `cluster_policies` · `instance_pools` · `sql_warehouses`

**Unity Catalog:** `external_locations` · `storage_credentials` · `connections`

**Workspace Settings:** `workspace_conf_legacy` · `workspace_settings_v2` · `workspace_settings_typed` · `sql_global_config` · `global_init_scripts` · `ip_access_lists` · `secret_scopes` · `tokens`

**SQL Assets:** `sql_queries` · `sql_alerts`

---

## Service Principals (production use)

Service principals are the recommended way to run this script in production — they don't expire like PAT tokens, can be managed centrally, and work in workspaces where PAT tokens are disabled.

**`workspace_inventory_api.py`** does not support service principals — it only accepts PAT tokens. Use this script for Dev and UAT environments where PAT tokens are available.

**`workspace_inventory_sdk.py`** and **`workspace_config_inventory_sdk.py`** fully support service principals via OAuth M2M (machine-to-machine). Use these scripts for production environments where PAT tokens are disabled and access must go through a service principal.

### Option A — environment variables

```bash
DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net \
DATABRICKS_CLIENT_ID=<sp-client-id> \
DATABRICKS_CLIENT_SECRET=<sp-secret> \
python3 workspace_inventory_sdk.py --save

DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net \
DATABRICKS_CLIENT_ID=<sp-client-id> \
DATABRICKS_CLIENT_SECRET=<sp-secret> \
python3 workspace_config_inventory_sdk.py --save
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
python3 workspace_inventory_sdk.py --profile prod-sp --save
python3 workspace_config_inventory_sdk.py --profile prod-sp --save
```

### Permissions required

The service principal needs the following access on the target workspace:

| Resource | Required permission |
|---|---|
| Jobs, pipelines, notebooks, repos, apps | Workspace read access (CAN VIEW) |
| Tables, volumes, functions, models | `USE CATALOG` + `USE SCHEMA` on each catalog/schema |
| Serving endpoints, experiments, dashboards | Workspace read access |
| Users, groups, service principals | Workspace admin (or user admin role) |
| Clusters, SQL warehouses, cluster policies | Workspace read access |
| Workspace settings, IP access lists, tokens | Workspace admin |
| Secret scopes | Access to individual scopes (or admin for all) |
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
