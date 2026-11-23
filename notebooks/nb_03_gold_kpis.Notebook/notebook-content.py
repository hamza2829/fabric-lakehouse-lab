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

# # Gold: star schema and KPIs
#
# Builds the tables the Power BI semantic model reads in Direct Lake mode:
#
# | table | grain |
# |---|---|
# | `dim_date`, `dim_material`, `dim_supplier`, `dim_machine` | one row per member |
# | `fact_orders` | order line |
# | `fact_production` | machine run, with scrap aggregated per run |
# | `fact_inspections` | inspection |
# | `fact_complaints` | complaint |
# | `gold_kpi_summary` | one row per KPI, names identical to the DAX measures |
# | `gold_kpi_monthly` | one row per month |
#
# The transformations are Spark SQL views; a PySpark cell writes them as Delta tables.

# PARAMETERS CELL ********************

batch_id = "manual"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dimensions

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_dim_date AS
# MAGIC WITH bounds AS (
# MAGIC   SELECT
# MAGIC     trunc(LEAST((SELECT MIN(order_date) FROM silver_orders),
# MAGIC                 (SELECT MIN(run_date) FROM silver_machine_runs)), 'MM') AS d0,
# MAGIC     last_day(GREATEST((SELECT MAX(requested_date) FROM silver_orders),
# MAGIC                       (SELECT MAX(run_date) FROM silver_machine_runs),
# MAGIC                       (SELECT MAX(to_date(received_at)) FROM silver_complaints))) AS d1
# MAGIC )
# MAGIC SELECT
# MAGIC   d                                        AS date,
# MAGIC   CAST(date_format(d, 'yyyyMMdd') AS INT)  AS date_key,
# MAGIC   CAST(year(d) AS INT)                     AS year,
# MAGIC   CAST(quarter(d) AS INT)                  AS quarter,
# MAGIC   CAST(month(d) AS INT)                    AS month,
# MAGIC   date_format(d, 'MMM')                    AS month_name,
# MAGIC   date_format(d, 'yyyy-MM')                AS year_month,
# MAGIC   CAST(weekofyear(d) AS INT)               AS iso_week,
# MAGIC   CAST(dayofweek(d) AS INT)                AS day_of_week,
# MAGIC   dayofweek(d) IN (1, 7)                   AS is_weekend
# MAGIC FROM bounds
# MAGIC LATERAL VIEW explode(sequence(d0, d1, INTERVAL 1 DAY)) t AS d;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_dim_material AS
# MAGIC SELECT material_id, material_name, material_type, material_group, base_unit,
# MAGIC        net_weight_kg, standard_cost_eur, list_price_eur, supplier_id
# MAGIC FROM silver_materials;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_dim_supplier AS
# MAGIC SELECT supplier_id, supplier_name, country_code, rating, lead_time_days
# MAGIC FROM silver_suppliers;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_dim_machine AS
# MAGIC SELECT machine_id, machine_name, machine_type, line, plant, ideal_cycle_time_s, commissioned_year
# MAGIC FROM silver_machines

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Facts

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_fact_orders AS
# MAGIC SELECT
# MAGIC   order_id, order_line, customer_id, material_id, plant,
# MAGIC   order_date, requested_date, delivered_date,
# MAGIC   quantity_pc, net_value_eur,
# MAGIC   delivered_date IS NOT NULL AS is_delivered,
# MAGIC   CASE WHEN delivered_date IS NULL THEN NULL ELSE delivered_date <= requested_date END AS is_on_time,
# MAGIC   CASE WHEN delivered_date IS NULL THEN NULL
# MAGIC        ELSE CAST(datediff(delivered_date, requested_date) AS INT) END AS delivery_delay_days,
# MAGIC   is_late_arrival
# MAGIC FROM silver_orders;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_fact_production AS
# MAGIC SELECT
# MAGIC   r.run_id, r.machine_id, r.material_id, r.shift, r.run_date,
# MAGIC   r.planned_runtime_min, r.downtime_min, r.run_time_min,
# MAGIC   m.ideal_cycle_time_s,
# MAGIC   CAST(r.units_produced * m.ideal_cycle_time_s AS DOUBLE) / 60 AS ideal_run_time_min,
# MAGIC   r.units_produced,
# MAGIC   CAST(COALESCE(s.scrap_units, 0) AS BIGINT) AS scrap_units,
# MAGIC   CAST(r.units_produced - COALESCE(s.scrap_units, 0) AS BIGINT) AS good_units,
# MAGIC   r.is_late_arrival
# MAGIC FROM silver_machine_runs r
# MAGIC JOIN silver_machines m ON m.machine_id = r.machine_id
# MAGIC LEFT JOIN (
# MAGIC   SELECT run_id, SUM(scrap_qty_pc) AS scrap_units FROM silver_scrap_events GROUP BY run_id
# MAGIC ) s ON s.run_id = r.run_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_fact_inspections AS
# MAGIC SELECT
# MAGIC   i.inspection_id, i.run_id, r.machine_id, i.material_id,
# MAGIC   to_date(i.inspected_at) AS inspection_date,
# MAGIC   i.sample_size, i.defects_found, i.result,
# MAGIC   i.result = 'PASS' AS is_pass
# MAGIC FROM silver_inspections i
# MAGIC JOIN silver_machine_runs r ON r.run_id = i.run_id;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_fact_complaints AS
# MAGIC SELECT complaint_id, order_id, order_line, material_id,
# MAGIC        to_date(received_at) AS complaint_date,
# MAGIC        category, severity, status, qty_affected
# MAGIC FROM silver_complaints

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## KPIs
#
# Every KPI below has a DAX measure of the same name in the semantic model, built from the
# same columns, so `gold_kpi_summary` doubles as the expected values for the model tests.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TEMP VIEW v_kpi_base AS
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM v_fact_orders)                 AS order_lines,
# MAGIC   (SELECT SUM(net_value_eur) FROM v_fact_orders)       AS net_sales_eur,
# MAGIC   (SELECT COUNT_IF(is_delivered) FROM v_fact_orders)   AS delivered_lines,
# MAGIC   (SELECT COUNT_IF(is_on_time) FROM v_fact_orders)     AS on_time_lines,
# MAGIC   (SELECT SUM(units_produced) FROM v_fact_production)  AS units_produced,
# MAGIC   (SELECT SUM(scrap_units) FROM v_fact_production)     AS scrap_units,
# MAGIC   (SELECT SUM(good_units) FROM v_fact_production)      AS good_units,
# MAGIC   (SELECT SUM(planned_runtime_min) FROM v_fact_production) AS planned_min,
# MAGIC   (SELECT SUM(run_time_min) FROM v_fact_production)    AS run_min,
# MAGIC   (SELECT SUM(ideal_run_time_min) FROM v_fact_production) AS ideal_min,
# MAGIC   (SELECT COUNT(*) FROM v_fact_complaints)             AS complaints,
# MAGIC   (SELECT COUNT(*) FROM v_fact_inspections)            AS inspections,
# MAGIC   (SELECT COUNT_IF(is_pass) FROM v_fact_inspections)   AS passed_inspections;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_kpi_summary AS
# MAGIC SELECT stack(15,
# MAGIC   'Order Lines',            CAST(order_lines AS DOUBLE),                              'Delivery',   '#,0',
# MAGIC   'Net Sales EUR',          CAST(net_sales_eur AS DOUBLE),                            'Delivery',   '#,0.00',
# MAGIC   'Delivered Lines',        CAST(delivered_lines AS DOUBLE),                          'Delivery',   '#,0',
# MAGIC   'On-Time Lines',          CAST(on_time_lines AS DOUBLE),                            'Delivery',   '#,0',
# MAGIC   'On-Time Delivery %',     CAST(on_time_lines AS DOUBLE) / delivered_lines,          'Delivery',   '0.0%',
# MAGIC   'Units Produced',         CAST(units_produced AS DOUBLE),                           'Production', '#,0',
# MAGIC   'Scrap Units',            CAST(scrap_units AS DOUBLE),                              'Production', '#,0',
# MAGIC   'Scrap Rate %',           CAST(scrap_units AS DOUBLE) / units_produced,             'Production', '0.00%',
# MAGIC   'Availability %',         run_min / planned_min,                                    'OEE',        '0.0%',
# MAGIC   'Performance %',          ideal_min / run_min,                                      'OEE',        '0.0%',
# MAGIC   'Quality Rate %',         CAST(good_units AS DOUBLE) / units_produced,              'OEE',        '0.0%',
# MAGIC   'OEE %',                  (run_min / planned_min) * (ideal_min / run_min)
# MAGIC                               * (CAST(good_units AS DOUBLE) / units_produced),        'OEE',        '0.0%',
# MAGIC   'Complaints',              CAST(complaints AS DOUBLE),                               'Quality',    '#,0',
# MAGIC   'Complaint Rate %',       CAST(complaints AS DOUBLE) / delivered_lines,             'Quality',    '0.00%',
# MAGIC   'Inspection Pass Rate %', CAST(passed_inspections AS DOUBLE) / inspections,         'Quality',    '0.0%'
# MAGIC ) AS (kpi_name, kpi_value, kpi_area, value_format)
# MAGIC FROM v_kpi_base;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_kpi_monthly AS
# MAGIC WITH o AS (
# MAGIC   SELECT date_format(order_date, 'yyyy-MM') AS year_month, COUNT(*) AS order_lines,
# MAGIC          COUNT_IF(is_delivered) AS delivered_lines, COUNT_IF(is_on_time) AS on_time_lines
# MAGIC   FROM v_fact_orders GROUP BY date_format(order_date, 'yyyy-MM')
# MAGIC ), p AS (
# MAGIC   SELECT date_format(run_date, 'yyyy-MM') AS year_month, SUM(units_produced) AS units_produced,
# MAGIC          SUM(scrap_units) AS scrap_units, SUM(good_units) AS good_units,
# MAGIC          SUM(planned_runtime_min) AS planned_min, SUM(run_time_min) AS run_min,
# MAGIC          SUM(ideal_run_time_min) AS ideal_min
# MAGIC   FROM v_fact_production GROUP BY date_format(run_date, 'yyyy-MM')
# MAGIC ), c AS (
# MAGIC   SELECT date_format(complaint_date, 'yyyy-MM') AS year_month, COUNT(*) AS complaints
# MAGIC   FROM v_fact_complaints GROUP BY date_format(complaint_date, 'yyyy-MM')
# MAGIC ), months AS (
# MAGIC   SELECT year_month FROM o UNION SELECT year_month FROM p UNION SELECT year_month FROM c
# MAGIC )
# MAGIC SELECT
# MAGIC   m.year_month,
# MAGIC   COALESCE(o.order_lines, 0)                               AS order_lines,
# MAGIC   COALESCE(o.delivered_lines, 0)                           AS delivered_lines,
# MAGIC   COALESCE(o.on_time_lines, 0)                             AS on_time_lines,
# MAGIC   CAST(o.on_time_lines AS DOUBLE) / o.delivered_lines      AS on_time_delivery_pct,
# MAGIC   COALESCE(p.units_produced, 0)                            AS units_produced,
# MAGIC   COALESCE(p.scrap_units, 0)                               AS scrap_units,
# MAGIC   CAST(p.scrap_units AS DOUBLE) / p.units_produced         AS scrap_rate_pct,
# MAGIC   p.run_min / p.planned_min                                AS availability_pct,
# MAGIC   p.ideal_min / p.run_min                                  AS performance_pct,
# MAGIC   CAST(p.good_units AS DOUBLE) / p.units_produced          AS quality_rate_pct,
# MAGIC   (p.run_min / p.planned_min) * (p.ideal_min / p.run_min)
# MAGIC     * (CAST(p.good_units AS DOUBLE) / p.units_produced)    AS oee_pct,
# MAGIC   COALESCE(c.complaints, 0)                                AS complaints,
# MAGIC   CAST(COALESCE(c.complaints, 0) AS DOUBLE) / o.delivered_lines AS complaint_rate_pct
# MAGIC FROM months m
# MAGIC LEFT JOIN o ON o.year_month = m.year_month
# MAGIC LEFT JOIN p ON p.year_month = m.year_month
# MAGIC LEFT JOIN c ON c.year_month = m.year_month

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Write gold Delta tables

# CELL ********************

import json

GOLD_TABLES = {
    "dim_date": "v_dim_date",
    "dim_material": "v_dim_material",
    "dim_supplier": "v_dim_supplier",
    "dim_machine": "v_dim_machine",
    "fact_orders": "v_fact_orders",
    "fact_production": "v_fact_production",
    "fact_inspections": "v_fact_inspections",
    "fact_complaints": "v_fact_complaints",
    "gold_kpi_summary": "v_kpi_summary",
    "gold_kpi_monthly": "v_kpi_monthly",
}
SORT_KEYS = {"gold_kpi_monthly": ["year_month"], "dim_date": ["date"], "gold_kpi_summary": []}

row_counts = {}
for table, view in GOLD_TABLES.items():
    df = spark.table(view)
    if table in SORT_KEYS:
        df = (df.orderBy(*SORT_KEYS[table]) if SORT_KEYS[table] else df).coalesce(1)  # small tables: one ordered file
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"Tables/{table}")
    row_counts[table] = spark.read.format("delta").load(f"Tables/{table}").count()
    print(f"{table:<20} {row_counts[table]:>7} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Referential integrity of the star schema
#
# Every relationship in the semantic model must resolve, otherwise Power BI would show a
# blank member. The cell fails the notebook (and the pipeline) on any orphan.

# CELL ********************

RELATIONSHIPS = [
    ("fact_orders", "material_id", "dim_material", "material_id"),
    ("fact_orders", "order_date", "dim_date", "date"),
    ("fact_production", "material_id", "dim_material", "material_id"),
    ("fact_production", "machine_id", "dim_machine", "machine_id"),
    ("fact_production", "run_date", "dim_date", "date"),
    ("fact_inspections", "material_id", "dim_material", "material_id"),
    ("fact_inspections", "machine_id", "dim_machine", "machine_id"),
    ("fact_inspections", "inspection_date", "dim_date", "date"),
    ("fact_complaints", "material_id", "dim_material", "material_id"),
    ("fact_complaints", "complaint_date", "dim_date", "date"),
    ("dim_material", "supplier_id", "dim_supplier", "supplier_id"),
]
orphans = {}
for f_tab, f_col, d_tab, d_col in RELATIONSHIPS:
    n = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {f_tab} f
        LEFT ANTI JOIN {d_tab} d ON f.{f_col} = d.{d_col}
        WHERE f.{f_col} IS NOT NULL
    """).first()["n"]
    orphans[f"{f_tab}.{f_col} -> {d_tab}.{d_col}"] = n
bad = {k: v for k, v in orphans.items() if v}
if bad:
    raise ValueError(f"orphan keys in gold: {bad}")
print("all relationships resolve:", len(orphans))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

kpis = {r["kpi_name"]: r["kpi_value"] for r in spark.read.format("delta").load("Tables/gold_kpi_summary").collect()}
notebookutils.notebook.exit(json.dumps({"layer": "gold", "row_counts": row_counts, "kpis": kpis}))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
