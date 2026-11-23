# How Fabric would map this

Every folder that ends in `.Notebook`, `.DataPipeline`, `.Lakehouse`, `.SemanticModel` or
`.Report` is a Fabric item in the layout Fabric git integration uses: a `.platform` file with the
item type, display name and logical id, plus the item definition. Connected to a workspace,
Fabric would create these items from the repo. That step was **not** performed for this lab
(no tenant); the mapping below is what it would look like.

| Repo | Fabric item | Notes |
|---|---|---|
| `lakehouse/lh_manufacturing.Lakehouse/` | Lakehouse `lh_manufacturing` | git holds only the item; data lives in OneLake. Locally: `onelake/ws_manufacturing/lh_manufacturing.Lakehouse/{Files,Tables}` |
| `notebooks/*.Notebook/notebook-content.py` | Notebooks | Fabric's own `.py` serialisation: `# CELL`, `# MARKDOWN`, `# PARAMETERS CELL`, `# METADATA`, `# MAGIC %%sql`. Default lakehouse set in the notebook metadata |
| `pipelines/*.DataPipeline/pipeline-content.json` | Data pipelines | `Copy`, `TridentNotebook` and `InvokePipeline` activities, pipeline parameters, retries and timeouts |
| `pipelines/pl_lakehouse_daily.DataPipeline/schedule.json` | Pipeline schedule | body of the job scheduler REST call; schedules are not part of the git definition, so a release step applies it |
| `pipelines/connections.local.json` | Connection | in Fabric: a file share connection through an on-premises data gateway; the GUID in the copy activities is that connection |
| `powerbi/ManufacturingKPIs.SemanticModel/` | Semantic model | TMDL, Direct Lake partitions on the lakehouse SQL endpoint (`expressions.tmdl` holds placeholders for the endpoint) |
| `powerbi/ManufacturingKPIs.Report/` | Report | PBIR format, bound to the semantic model by relative path |
| `quality/` | Notebook or pipeline step | in Fabric the same rules would run as a notebook after silver, writing the report to `Files/reports/` and failing the pipeline over the threshold |
| `.github/workflows/ci.yml` | Deployment checks | in a real setup a release pipeline would also run the Fabric deployment (git sync or deployment pipelines) after these checks |

## What changes on a real tenant

* **Paths**: none. The notebooks use relative `Files/...` and `Tables/...` paths, which resolve
  against the default lakehouse in Fabric and against the local lakehouse folder here.
* **Tables**: the notebooks write Delta folders under `Tables/`; Fabric registers them as
  lakehouse tables automatically, the local runner does the same in the Spark catalog.
* **Ids**: the logical ids in `.platform` and the notebook/pipeline references stay as they are;
  `workspaceId` values are placeholders that Fabric rewrites on sync.
* **Direct Lake endpoint**: `expressions.tmdl` needs the lakehouse SQL endpoint and id.
* **Copy source**: the plant file share connection and gateway have to exist.
* **Medallion split**: one lakehouse with `bronze_`, `silver_` prefixes keeps the lab small. A
  production setup would usually separate bronze, silver and gold into their own lakehouses or
  workspaces, which changes table references from names to shortcuts or three part names.
* **Incremental loads**: bronze overwrites per load date to stay idempotent. With real volumes,
  bronze would append per batch and silver would `MERGE` on the business key.

## What is deliberately not emulated

* Capacity, security, workspace roles, OneLake shortcuts, Spark pools and environments.
* Expression language coverage beyond what the pipelines use (see `fabric_local/expressions.py`).
* Power BI rendering. The PBIP files follow the published PBIP/TMDL/PBIR formats and are checked
  by tests for internal consistency and against the gold data, but they were not opened in
  Power BI Desktop or deployed to a workspace.
