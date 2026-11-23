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

# # Silver: clean and conform
#
# For every source entity, in dependency order (suppliers, materials, orders, machines,
# machine runs, scrap events, inspections, complaints):
#
# 1. **type** the bronze strings (dates, timestamps, numbers), empty strings become NULL
# 2. **conform units**: order quantities to pieces (`PCS`, `pc`, `Stk`, `DZ`), run times to
#    minutes (`h`), scrap weights in `KG` to pieces via the material net weight
# 3. **validate** with ordered rules; the first failing rule names the quarantine reason:
#    `missing_key`, `orphan_fk`, `unknown_unit`, `out_of_range`, `future_date`, `invalid_value`
# 4. **deduplicate** on the business key, newest extract wins
# 5. **flag** late arriving records instead of dropping them
#
# Rejected rows go to `silver_quarantine` with the reason and the full record as JSON, and
# `silver_load_stats` keeps the per entity row accounting.

# PARAMETERS CELL ********************

load_date = "2026-09-30"
as_of = "2026-09-30T23:59:59"
batch_id = "manual"
late_threshold_hours = 48
order_late_threshold_hours = 72

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from functools import reduce

from pyspark.sql import DataFrame, Row, Window
from pyspark.sql import functions as F

AS_OF_TS = F.to_timestamp(F.lit(as_of))
AS_OF_DATE = F.to_date(F.lit(as_of[:10]))
quarantine_parts: list[DataFrame] = []
load_stats: dict[str, dict] = {}


def bronze(name: str) -> DataFrame:
    return spark.read.format("delta").load(f"Tables/{name}")


def s(col: str):
    """Trimmed string; empty strings become NULL."""
    c = F.trim(F.col(col))
    return F.when(c == "", F.lit(None)).otherwise(c)


def num(col: str, dtype: str = "double"):
    return s(col).cast(dtype)


def ts(col: str):
    return F.to_timestamp(s(col))


def dt(col: str):
    return F.to_date(s(col))


def hours_between(later, earlier):
    return (F.unix_timestamp(later) - F.unix_timestamp(earlier)) / 3600.0


def missing(*cols: str):
    return reduce(lambda a, b: a | b, [F.col(c).isNull() for c in cols])


def flag_orphans(df: DataFrame, cols: list[str], ref: DataFrame, ref_cols: list[str], flag: str) -> DataFrame:
    """Add boolean ``flag``: the key is present but has no match in ``ref``."""
    aliases = [f"__ref_{i}" for i in range(len(ref_cols))]
    keys = ref.select(*[F.col(r).alias(a) for r, a in zip(ref_cols, aliases)]).distinct().withColumn("__hit", F.lit(True))
    cond = reduce(lambda a, b: a & b, [F.col(c) == F.col(a) for c, a in zip(cols, aliases)])
    present = reduce(lambda a, b: a & b, [F.col(c).isNotNull() for c in cols])
    return (df.join(F.broadcast(keys), cond, "left")
              .withColumn(flag, present & F.col("__hit").isNull())
              .drop("__hit", *aliases))


def apply_rules(df: DataFrame, entity: str, key_cols: list[str], rules: list[tuple]) -> DataFrame:
    """Route rows that fail a rule to quarantine; the first failing rule names the reason."""
    reason = F.lit(None).cast("string")
    for name, failed in reversed(rules):
        reason = F.when(F.coalesce(failed, F.lit(False)), F.lit(name)).otherwise(reason)
    df = df.withColumn("_dq_reason", reason).cache()
    data_cols = [c for c in df.columns if not c.startswith("_")]
    quarantine_parts.append(
        df.filter(F.col("_dq_reason").isNotNull()).select(
            F.lit(entity).alias("entity"),
            F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in key_cols]).alias("business_key"),
            F.col("_dq_reason").alias("dq_reason"),
            F.to_json(F.struct(*[F.col(c) for c in data_cols])).alias("record_json"),
        )
    )
    good = df.filter(F.col("_dq_reason").isNull())
    return good.drop(*[c for c in good.columns if c.startswith("_")])


def dedupe(df: DataFrame, key_cols: list[str], order_col: str) -> DataFrame:
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("__rn", F.row_number().over(w)).filter("__rn = 1").drop("__rn")


def write_silver(df: DataFrame, table: str, entity: str, bronze_rows: int, valid_rows: int,
                 late_col: str | None = None, converted=None) -> DataFrame:
    (df.withColumn("batch_id", F.lit(batch_id))
       .withColumn("processed_at", F.current_timestamp())
       .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .save(f"Tables/{table}"))
    out = spark.read.format("delta").load(f"Tables/{table}")
    n = out.count()
    load_stats[entity] = {
        "bronze_rows": bronze_rows,
        "quarantined": bronze_rows - valid_rows,
        "duplicates_removed": valid_rows - n,
        "silver_rows": n,
        "late_flagged": out.filter(F.col(late_col)).count() if late_col else 0,
        "units_converted": out.filter(converted).count() if converted is not None else 0,
    }
    print(f"{table:<24} {load_stats[entity]}")
    return out

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## ERP: suppliers, materials, orders

# CELL ********************

b = bronze("bronze_erp_suppliers")
n_bronze = b.count()
sup = b.select(
    s("supplier_id").alias("supplier_id"),
    s("supplier_name").alias("supplier_name"),
    F.upper(s("country")).alias("country_code"),
    num("rating").alias("rating"),
    num("lead_time_days", "int").alias("lead_time_days"),
    ts("extracted_at").alias("extracted_at"),
)
sup = apply_rules(sup, "suppliers", ["supplier_id"], [
    ("missing_key", missing("supplier_id")),
    ("out_of_range", (F.col("rating") < 1) | (F.col("rating") > 5) | (F.col("lead_time_days") < 0)),
])
silver_suppliers = write_silver(dedupe(sup, ["supplier_id"], "extracted_at"),
                                "silver_suppliers", "suppliers", n_bronze, sup.count())

b = bronze("bronze_erp_materials")
n_bronze = b.count()
mat = b.select(
    s("material_id").alias("material_id"),
    s("material_name").alias("material_name"),
    F.upper(s("material_type")).alias("material_type"),
    s("material_group").alias("material_group"),
    F.upper(s("base_unit")).alias("base_unit"),
    num("net_weight_kg").alias("net_weight_kg"),
    num("standard_cost_eur").alias("standard_cost_eur"),
    num("list_price_eur").alias("list_price_eur"),
    s("supplier_id").alias("supplier_id"),
)
mat = flag_orphans(mat, ["supplier_id"], silver_suppliers, ["supplier_id"], "_orphan_supplier")
mat = apply_rules(mat, "materials", ["material_id"], [
    ("missing_key", missing("material_id", "material_type")),
    ("orphan_fk", F.col("_orphan_supplier")),
    ("out_of_range", (F.col("standard_cost_eur") < 0)
        | ((F.col("material_type") == "FG") & ~(F.coalesce(F.col("net_weight_kg"), F.lit(0.0)) > 0))),
])
silver_materials = write_silver(dedupe(mat, ["material_id"], "material_name"),
                                "silver_materials", "materials", n_bronze, mat.count())

ORDER_UNITS = {"PC": 1, "PCS": 1, "STK": 1, "ST": 1, "EA": 1, "DZ": 12}
order_unit_map = F.create_map(*[F.lit(x) for kv in ORDER_UNITS.items() for x in kv])

b = bronze("bronze_erp_orders")
n_bronze = b.count()
orders = (
    b.select(
        s("order_id").alias("order_id"),
        num("order_line", "int").alias("order_line"),
        s("customer_id").alias("customer_id"),
        s("material_id").alias("material_id"),
        s("plant").alias("plant"),
        dt("order_date").alias("order_date"),
        dt("requested_date").alias("requested_date"),
        dt("delivered_date").alias("delivered_date"),
        num("quantity").alias("source_quantity"),
        s("unit").alias("source_unit"),
        num("unit_price_eur").alias("unit_price_eur"),
        num("net_value_eur").alias("net_value_eur"),
        F.upper(s("currency")).alias("currency"),
        ts("extracted_at").alias("extracted_at"),
    )
    .withColumn("_unit_factor", order_unit_map[F.upper(F.col("source_unit"))])
    .withColumn("quantity_pc", F.round(F.col("source_quantity") * F.col("_unit_factor")).cast("long"))
    .withColumn("is_late_arrival",
                hours_between(F.col("extracted_at"), F.col("order_date").cast("timestamp")) > order_late_threshold_hours)
)
orders = flag_orphans(orders, ["material_id"], silver_materials, ["material_id"], "_orphan_material")
orders = apply_rules(orders, "orders", ["order_id", "order_line"], [
    ("missing_key", missing("order_id", "order_line", "material_id")),
    ("orphan_fk", F.col("_orphan_material")),
    ("unknown_unit", F.col("_unit_factor").isNull()),
    ("out_of_range", (F.col("quantity_pc") <= 0) | (F.col("net_value_eur") < 0)),
    ("future_date", F.col("order_date") > AS_OF_DATE),
    ("invalid_value", (F.col("requested_date") < F.col("order_date"))
        | (F.col("delivered_date") < F.col("order_date")) | (F.col("currency") != "EUR")),
])
silver_orders = write_silver(dedupe(orders, ["order_id", "order_line"], "extracted_at"),
                             "silver_orders", "orders", n_bronze, orders.count(),
                             late_col="is_late_arrival", converted=F.col("source_unit") != "PC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Production: machines, machine runs, scrap events

# CELL ********************

b = bronze("bronze_prod_machines")
n_bronze = b.count()
machines = b.select(
    s("machine_id").alias("machine_id"),
    s("machine_name").alias("machine_name"),
    s("machine_type").alias("machine_type"),
    s("line").alias("line"),
    s("plant").alias("plant"),
    num("ideal_cycle_time_s", "int").alias("ideal_cycle_time_s"),
    num("commissioned_year", "int").alias("commissioned_year"),
)
machines = apply_rules(machines, "machines", ["machine_id"], [
    ("missing_key", missing("machine_id")),
    ("out_of_range", ~(F.coalesce(F.col("ideal_cycle_time_s"), F.lit(0)) > 0)),
])
silver_machines = write_silver(dedupe(machines, ["machine_id"], "machine_name"),
                               "silver_machines", "machines", n_bronze, machines.count())

TIME_UNITS = {"MIN": 1.0, "H": 60.0, "S": 1.0 / 60.0}
time_unit_map = F.create_map(*[F.lit(x) for kv in TIME_UNITS.items() for x in kv])

b = bronze("bronze_prod_machine_runs")
n_bronze = b.count()
runs = (
    b.select(
        s("run_id").alias("run_id"),
        s("machine_id").alias("machine_id"),
        s("material_id").alias("material_id"),
        s("shift").alias("shift"),
        ts("start_ts").alias("start_ts"),
        ts("end_ts").alias("end_ts"),
        num("planned_runtime").alias("source_planned_runtime"),
        num("downtime").alias("source_downtime"),
        s("time_unit").alias("source_time_unit"),
        num("units_produced", "long").alias("units_produced"),
        ts("recorded_at").alias("recorded_at"),
    )
    .withColumn("_time_factor", time_unit_map[F.upper(F.col("source_time_unit"))])
    .withColumn("planned_runtime_min", F.round(F.col("source_planned_runtime") * F.col("_time_factor"), 1))
    .withColumn("downtime_min", F.round(F.col("source_downtime") * F.col("_time_factor"), 1))
    .withColumn("run_time_min", F.col("planned_runtime_min") - F.col("downtime_min"))
    .withColumn("run_date", F.to_date("start_ts"))
    .withColumn("is_late_arrival", hours_between(F.col("recorded_at"), F.col("end_ts")) > late_threshold_hours)
)
runs = flag_orphans(runs, ["machine_id"], silver_machines, ["machine_id"], "_orphan_machine")
runs = flag_orphans(runs, ["material_id"], silver_materials, ["material_id"], "_orphan_material")
runs = apply_rules(runs, "machine_runs", ["run_id"], [
    ("missing_key", missing("run_id", "machine_id", "material_id")),
    ("orphan_fk", F.col("_orphan_machine") | F.col("_orphan_material")),
    ("unknown_unit", F.col("_time_factor").isNull()),
    ("out_of_range", (F.col("planned_runtime_min") <= 0) | (F.col("downtime_min") < 0)
        | (F.col("downtime_min") > F.col("planned_runtime_min")) | (F.col("units_produced") < 0)
        | (F.col("end_ts") <= F.col("start_ts"))),
    ("future_date", F.col("start_ts") > AS_OF_TS),
])
silver_runs = write_silver(dedupe(runs, ["run_id"], "recorded_at"),
                           "silver_machine_runs", "machine_runs", n_bronze, runs.count(),
                           late_col="is_late_arrival", converted=F.lower(F.col("source_time_unit")) != "min")

weights = silver_materials.select(F.col("material_id").alias("_w_material"), F.col("net_weight_kg").alias("_weight_kg"))
b = bronze("bronze_prod_scrap_events")
n_bronze = b.count()
scrap = b.select(
    s("scrap_id").alias("scrap_id"),
    s("run_id").alias("run_id"),
    s("material_id").alias("material_id"),
    ts("event_ts").alias("event_ts"),
    num("scrap_qty").alias("source_qty"),
    s("unit").alias("source_unit"),
    s("reason_code").alias("reason_code"),
    ts("recorded_at").alias("recorded_at"),
)
scrap = (
    scrap.join(F.broadcast(weights), F.col("material_id") == F.col("_w_material"), "left")
    .withColumn("scrap_qty_pc",
                F.when(F.upper(F.col("source_unit")) == "PC", F.round(F.col("source_qty")))
                 .when(F.upper(F.col("source_unit")) == "KG", F.round(F.col("source_qty") / F.col("_weight_kg")))
                 .cast("long"))
    .withColumn("is_late_arrival", hours_between(F.col("recorded_at"), F.col("event_ts")) > late_threshold_hours)
)
scrap = flag_orphans(scrap, ["run_id"], silver_runs, ["run_id"], "_orphan_run")
scrap = apply_rules(scrap, "scrap_events", ["scrap_id"], [
    ("missing_key", missing("scrap_id", "run_id")),
    ("orphan_fk", F.col("_orphan_run")),
    ("unknown_unit", F.col("scrap_qty_pc").isNull()),
    ("out_of_range", F.col("scrap_qty_pc") <= 0),
])
silver_scrap = write_silver(dedupe(scrap, ["scrap_id"], "recorded_at"),
                            "silver_scrap_events", "scrap_events", n_bronze, scrap.count(),
                            late_col="is_late_arrival", converted=F.upper(F.col("source_unit")) != "PC")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Quality: inspections, complaints

# CELL ********************

b = bronze("bronze_qm_inspections")
n_bronze = b.count()
insp = (
    b.select(
        s("inspection_id").alias("inspection_id"),
        s("run_id").alias("run_id"),
        s("material_id").alias("material_id"),
        ts("inspected_at").alias("inspected_at"),
        s("inspector").alias("inspector"),
        num("sample_size", "int").alias("sample_size"),
        num("defects_found", "int").alias("defects_found"),
        F.upper(s("result")).alias("result"),
        ts("recorded_at").alias("recorded_at"),
    )
    .withColumn("is_late_arrival", hours_between(F.col("recorded_at"), F.col("inspected_at")) > late_threshold_hours)
)
insp = flag_orphans(insp, ["run_id"], silver_runs, ["run_id"], "_orphan_run")
insp = apply_rules(insp, "inspections", ["inspection_id"], [
    ("missing_key", missing("inspection_id", "run_id")),
    ("orphan_fk", F.col("_orphan_run")),
    ("out_of_range", (F.col("sample_size") <= 0) | (F.col("defects_found") < 0)
        | (F.col("defects_found") > F.col("sample_size"))),
    ("invalid_value", ~F.coalesce(F.col("result").isin("PASS", "FAIL"), F.lit(False))),
    ("future_date", F.col("inspected_at") > AS_OF_TS),
])
silver_inspections = write_silver(dedupe(insp, ["inspection_id"], "recorded_at"),
                                  "silver_inspections", "inspections", n_bronze, insp.count(),
                                  late_col="is_late_arrival")

b = bronze("bronze_qm_complaints")
n_bronze = b.count()
compl = b.select(
    s("complaint_id").alias("complaint_id"),
    s("order_id").alias("order_id"),
    num("order_line", "int").alias("order_line"),
    s("material_id").alias("material_id"),
    ts("received_at").alias("received_at"),
    s("category").alias("category"),
    F.lower(s("severity")).alias("severity"),
    num("qty_affected", "int").alias("qty_affected"),
    F.lower(s("status")).alias("status"),
)
compl = flag_orphans(compl, ["order_id", "order_line"], silver_orders, ["order_id", "order_line"], "_orphan_order")
compl = apply_rules(compl, "complaints", ["complaint_id"], [
    ("missing_key", missing("complaint_id", "order_id", "order_line")),
    ("orphan_fk", F.col("_orphan_order")),
    ("out_of_range", F.col("qty_affected") <= 0),
    ("future_date", F.col("received_at") > AS_OF_TS),
])
silver_complaints = write_silver(dedupe(compl, ["complaint_id"], "received_at"),
                                 "silver_complaints", "complaints", n_bronze, compl.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Quarantine and load statistics

# CELL ********************

quarantine = (reduce(DataFrame.unionByName, quarantine_parts)
              .withColumn("batch_id", F.lit(batch_id))
              .withColumn("quarantined_at", F.current_timestamp()))
quarantine.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("Tables/silver_quarantine")

stats_rows = [Row(entity=k, **{m: int(v) for m, v in vals.items()}) for k, vals in load_stats.items()]
(spark.createDataFrame(stats_rows)
      .withColumn("batch_id", F.lit(batch_id))
      .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
      .save("Tables/silver_load_stats"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT entity, dq_reason, COUNT(*) AS quarantined_rows
# MAGIC FROM silver_quarantine
# MAGIC GROUP BY entity, dq_reason
# MAGIC ORDER BY entity, dq_reason

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

reasons = {}
for r in spark.read.format("delta").load("Tables/silver_quarantine").groupBy("entity", "dq_reason").count().collect():
    reasons.setdefault(r["entity"], {})[r["dq_reason"]] = r["count"]
notebookutils.notebook.exit(json.dumps({"layer": "silver", "load_stats": load_stats, "quarantine": reasons}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
