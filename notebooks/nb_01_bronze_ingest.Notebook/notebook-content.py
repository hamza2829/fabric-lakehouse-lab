# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "aff243db-abb3-5980-8c23-f25d978e466b",
# META       "default_lakehouse_name": "lh_manufacturing",
# META       "default_lakehouse_workspace_id": "de52839d-6e87-56b4-8a1a-88ee551e28af"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze: ingest raw source files
#
# Reads the files that `pl_ingest_sources` copied into `Files/landing/<source>/<load_date>/`
# and writes each one to a Delta table in `Tables/` **without changing a single value**.
#
# * every column is kept as a string, so malformed values survive for the quality checks
# * lineage columns are added: `_source_file`, `_ingested_at`, `_batch_id`, `_load_date`
# * the load is a full overwrite per batch, which keeps a rerun idempotent

# PARAMETERS CELL ********************

load_date = "2026-09-30"
batch_id = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

from pyspark.sql import functions as F

LANDING = "Files/landing"

# table name -> (format, path below Files/landing, extra reader options)
SOURCES = {
    "bronze_erp_suppliers": ("csv", f"erp/{load_date}/suppliers.csv", {}),
    "bronze_erp_materials": ("csv", f"erp/{load_date}/materials.csv", {}),
    "bronze_erp_orders": ("csv", f"erp/{load_date}/orders.csv", {}),
    "bronze_prod_machines": ("csv", f"production/{load_date}/machines.csv", {}),
    "bronze_prod_machine_runs": ("csv", f"production/{load_date}/machine_runs.csv", {}),
    "bronze_prod_scrap_events": ("json", f"production/{load_date}/scrap_events.jsonl", {}),
    "bronze_qm_inspections": ("json", f"quality/{load_date}/inspections.jsonl", {}),
    "bronze_qm_complaints": ("json", f"quality/{load_date}/complaints.json", {"multiLine": "true"}),
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_raw(fmt: str, path: str, options: dict):
    reader = spark.read.option("mode", "PERMISSIVE")
    if fmt == "csv":
        reader = reader.option("header", "true").option("inferSchema", "false").option("encoding", "UTF-8")
    else:
        # keep JSON numbers as text too: bronze never coerces types
        reader = reader.option("primitivesAsString", "true")
    for key, value in options.items():
        reader = reader.option(key, value)
    return reader.format(fmt).load(f"{LANDING}/{path}")


row_counts = {}
for table, (fmt, path, options) in SOURCES.items():
    df = (
        read_raw(fmt, path, options)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_load_date", F.lit(load_date))
    )
    (df.write.format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(f"Tables/{table}"))
    row_counts[table] = spark.read.format("delta").load(f"Tables/{table}").count()
    print(f"{table:<28} {row_counts[table]:>7} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Row counts per bronze table (Spark SQL)

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'erp' AS source, 'suppliers' AS entity, COUNT(*) AS row_count FROM bronze_erp_suppliers
# MAGIC UNION ALL SELECT 'erp', 'materials', COUNT(*) FROM bronze_erp_materials
# MAGIC UNION ALL SELECT 'erp', 'orders', COUNT(*) FROM bronze_erp_orders
# MAGIC UNION ALL SELECT 'production', 'machines', COUNT(*) FROM bronze_prod_machines
# MAGIC UNION ALL SELECT 'production', 'machine_runs', COUNT(*) FROM bronze_prod_machine_runs
# MAGIC UNION ALL SELECT 'production', 'scrap_events', COUNT(*) FROM bronze_prod_scrap_events
# MAGIC UNION ALL SELECT 'quality', 'inspections', COUNT(*) FROM bronze_qm_inspections
# MAGIC UNION ALL SELECT 'quality', 'complaints', COUNT(*) FROM bronze_qm_complaints

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit(json.dumps({"layer": "bronze", "load_date": load_date, "row_counts": row_counts}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
