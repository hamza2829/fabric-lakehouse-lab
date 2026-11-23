# Release notes

## v1.0.0 (2026-11-23)

First release of the lab.

### Added
* Seeded generator for ERP, production and quality sources (CSV, JSON lines, JSON array) with
  injected duplicates, missing keys, orphan references, unit mismatches, out of range values,
  future dated and late arriving records, plus a stale complaints feed. A manifest records every
  injected defect.
* Fabric notebooks in git format: `nb_01_bronze_ingest`, `nb_02_silver_clean_conform`,
  `nb_03_gold_kpis` (PySpark and Spark SQL).
* Data pipelines `pl_ingest_sources` (3 copy activities) and `pl_lakehouse_daily` (invoke
  pipeline, 3 notebook activities, daily schedule), with a validator and a local runner.
* Data quality rules for bronze and silver, a markdown report per source and a CI gate
  (bronze issue rate per source at most 10 %, no errors left in silver).
* Power BI project: Direct Lake semantic model in TMDL with 8 tables, 11 relationships and 15
  DAX measures, and a three page report in PBIR format.
* Documentation: architecture and data flow, data dictionary, KPI definitions, Fabric mapping,
  this branching and release guide.
* GitHub Actions workflow running the whole chain and uploading the quality report.

### Known limitations
* Not deployed to a Fabric tenant; the PBIP files were not opened in Power BI Desktop.
* Spark runs locally on Linux (CI). On Windows, Spark needs Hadoop's `winutils.exe`, which is
  not part of this repo.
