# Architecture and data flow

The lab follows the medallion pattern inside one Fabric lakehouse, `lh_manufacturing`.
Raw files land in the **Files** area, every layer after that is a Delta table in **Tables**.

```mermaid
flowchart LR
    subgraph src["Source systems (synthetic, data/generate_data.py)"]
        ERP["ERP<br/>suppliers.csv<br/>materials.csv<br/>orders.csv"]
        MES["Production / MES<br/>machines.csv<br/>machine_runs.csv<br/>scrap_events.jsonl"]
        QMS["Quality / QMS<br/>inspections.jsonl<br/>complaints.json"]
    end

    subgraph pl["pl_lakehouse_daily (Data Pipeline, daily 05:30)"]
        ING["InvokePipeline<br/>pl_ingest_sources<br/>3 x Copy activity"]
        NB1["Notebook<br/>nb_01_bronze_ingest"]
        NB2["Notebook<br/>nb_02_silver_clean_conform"]
        NB3["Notebook<br/>nb_03_gold_kpis"]
        ING --> NB1 --> NB2 --> NB3
    end

    subgraph lh["lh_manufacturing.Lakehouse"]
        FILES["Files/landing/{source}/{load_date}/"]
        BRZ["Tables/bronze_*<br/>8 tables, all text,<br/>+ lineage columns"]
        SLV["Tables/silver_*<br/>8 entities typed, deduplicated,<br/>units conformed"]
        QUA["Tables/silver_quarantine<br/>Tables/silver_load_stats"]
        GLD["Tables/dim_* and fact_*<br/>gold_kpi_summary<br/>gold_kpi_monthly"]
    end

    DQ["quality/run_quality.py<br/>rules.toml<br/>quality_report.md"]
    PBI["ManufacturingKPIs<br/>semantic model (TMDL, Direct Lake)<br/>15 DAX measures + report (PBIR)"]

    ERP & MES & QMS -->|Copy| FILES
    ING -.-> FILES
    FILES --> NB1 --> BRZ
    BRZ --> NB2 --> SLV
    NB2 --> QUA
    SLV --> NB3 --> GLD
    BRZ & SLV & QUA --> DQ
    GLD --> PBI
```

## Layers

| Layer | Where | What happens | Tables |
|---|---|---|---|
| Landing | `Files/landing/<source>/<load_date>/` | binary copy of the nightly extracts, nothing parsed | files only |
| Bronze | `Tables/bronze_*` | one Delta table per file, **every value kept as text**, plus `_source_file`, `_ingested_at`, `_batch_id`, `_load_date` | 8 |
| Silver | `Tables/silver_*` | typing, unit conversion, validation, quarantine, deduplication, late arrival flags | 8 entities + `silver_quarantine` + `silver_load_stats` |
| Gold | `Tables/dim_*`, `fact_*`, `gold_*` | star schema for the semantic model, KPI tables | 4 dims, 4 facts, 2 KPI tables |

## Silver processing order

Entities are processed so that every foreign key is checked against an already cleaned parent:

```mermaid
flowchart TD
    suppliers --> materials --> orders --> complaints
    machines --> machine_runs
    materials --> machine_runs --> scrap_events
    machine_runs --> inspections
```

For each entity the notebook applies ordered rules. The first failing rule names the
quarantine reason, so every rejected row has exactly one reason:

1. `missing_key`: a business or foreign key is NULL or empty
2. `orphan_fk`: a foreign key has no match in the cleaned parent
3. `unknown_unit`: a unit that has no conversion factor
4. `out_of_range`: negative quantities, downtime above planned time, defects above sample size
5. `future_date`: an event after the as-of time of the load
6. `invalid_value`: e.g. delivery before order, an unknown inspection result

Rows that pass are deduplicated on the business key (newest extract wins), units are converted
(`PCS`, `pc`, `Stk` to `PC`, `DZ` x 12, hours to minutes, scrap `KG` to pieces via the material
net weight) and late records are **kept** with `is_late_arrival = true`.

## Local runtime

`fabric_local/` stands in for the parts of Fabric that are not files:

| Fabric | Local stand-in |
|---|---|
| Spark runtime with Delta | `pyspark` + `delta-spark` from pip, `local[2]` |
| default lakehouse, relative `Files/` and `Tables/` paths | JVM working directory set to `onelake/ws_manufacturing/lh_manufacturing.Lakehouse` |
| lakehouse table discovery | every Delta folder under `Tables/` is registered in the session catalog after each cell |
| `notebookutils.notebook.exit` | raises an exit carrying the value, which the pipeline activity output exposes |
| pipeline orchestration | `fabric_local/pipeline.py`, topological order, retries, dependency conditions |
| connection to the plant file share | `pipelines/connections.local.json` maps the connection id to `data/raw` |
