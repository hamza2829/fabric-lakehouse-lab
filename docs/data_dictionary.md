# Data dictionary

Types are Spark SQL types as written to Delta. The semantic model maps them to TMDL types
(`STRING` to `string`, `INT`/`BIGINT` to `int64`, `DOUBLE` to `double`, `DATE` to `dateTime`,
`BOOLEAN` to `boolean`); a test compares the TMDL columns with the real gold Delta schema.

## Sources (landing and bronze)

Bronze tables keep the source columns **as text**, exactly as delivered, and add four lineage
columns: `_source_file`, `_ingested_at` (timestamp), `_batch_id` (pipeline run id), `_load_date`.

| Bronze table | Source file | Format | Grain |
|---|---|---|---|
| `bronze_erp_suppliers` | `erp/suppliers.csv` | CSV | supplier version per extract |
| `bronze_erp_materials` | `erp/materials.csv` | CSV | material |
| `bronze_erp_orders` | `erp/orders.csv` | CSV | sales order line |
| `bronze_prod_machines` | `production/machines.csv` | CSV | machine |
| `bronze_prod_machine_runs` | `production/machine_runs.csv` | CSV | machine run (one shift) |
| `bronze_prod_scrap_events` | `production/scrap_events.jsonl` | JSON lines | scrap booking |
| `bronze_qm_inspections` | `quality/inspections.jsonl` | JSON lines | in-process inspection |
| `bronze_qm_complaints` | `quality/complaints.json` | JSON array | customer complaint |

## Silver

Every silver table also has `batch_id` (STRING) and `processed_at` (TIMESTAMP).

### silver_suppliers (key `supplier_id`)
| Column | Type | Description |
|---|---|---|
| supplier_id | STRING | supplier number, `S-001` |
| supplier_name | STRING | legal name |
| country_code | STRING | ISO 3166 alpha-2 |
| rating | DOUBLE | supplier rating 1 to 5, latest extract |
| lead_time_days | INT | agreed replenishment lead time |
| extracted_at | TIMESTAMP | ERP extract time; the newest version wins |

### silver_materials (key `material_id`)
| Column | Type | Description |
|---|---|---|
| material_id | STRING | `FG-1001` finished good, `RM-2001` raw material |
| material_name | STRING | description |
| material_type | STRING | `FG` or `RM` |
| material_group | STRING | product family or raw material group |
| base_unit | STRING | `PC`, `KG` or `M` |
| net_weight_kg | DOUBLE | weight per piece, used to convert scrap booked in KG |
| standard_cost_eur | DOUBLE | standard cost per base unit |
| list_price_eur | DOUBLE | list price per piece (finished goods) |
| supplier_id | STRING | main supplier (raw materials), NULL for finished goods |

### silver_orders (key `order_id`, `order_line`)
| Column | Type | Description |
|---|---|---|
| order_id | STRING | sales order number |
| order_line | INT | line number (10, 20, ...) |
| customer_id | STRING | customer number |
| material_id | STRING | ordered finished good |
| plant | STRING | delivering plant |
| order_date | DATE | order entry date |
| requested_date | DATE | customer requested delivery date |
| delivered_date | DATE | goods issue date, NULL while open |
| source_quantity | DOUBLE | quantity as delivered by the ERP |
| source_unit | STRING | unit as delivered (`PC`, `PCS`, `pc`, `Stk`, `DZ`) |
| quantity_pc | BIGINT | quantity converted to pieces |
| unit_price_eur | DOUBLE | price per piece |
| net_value_eur | DOUBLE | net line value |
| currency | STRING | always `EUR` after validation |
| extracted_at | TIMESTAMP | ERP extract time |
| is_late_arrival | BOOLEAN | extracted more than 72 h after the order date |

### silver_machines (key `machine_id`)
| Column | Type | Description |
|---|---|---|
| machine_id | STRING | `M-01` |
| machine_name | STRING | display name |
| machine_type | STRING | CNC mill, press, ... |
| line | STRING | production line `L1` to `L4` |
| plant | STRING | plant code |
| ideal_cycle_time_s | INT | ideal seconds per unit |
| commissioned_year | INT | year of commissioning |

### silver_machine_runs (key `run_id`)
| Column | Type | Description |
|---|---|---|
| run_id | STRING | MES run number |
| machine_id | STRING | machine |
| material_id | STRING | produced finished good |
| shift | STRING | `A`, `B`, `C` |
| start_ts, end_ts | TIMESTAMP | run start and end |
| run_date | DATE | date of `start_ts` |
| source_planned_runtime, source_downtime | DOUBLE | times as booked |
| source_time_unit | STRING | `min` or `h` as booked |
| planned_runtime_min | DOUBLE | planned production time in minutes |
| downtime_min | DOUBLE | downtime in minutes |
| run_time_min | DOUBLE | planned minus downtime |
| units_produced | BIGINT | units produced in the run |
| recorded_at | TIMESTAMP | when the MES booked the run |
| is_late_arrival | BOOLEAN | booked more than 48 h after `end_ts` |

### silver_scrap_events (key `scrap_id`)
| Column | Type | Description |
|---|---|---|
| scrap_id | STRING | scrap booking number |
| run_id | STRING | machine run that produced the scrap |
| material_id | STRING | scrapped material |
| event_ts | TIMESTAMP | when the scrap occurred |
| source_qty, source_unit | DOUBLE, STRING | quantity and unit as booked (`PC` or `KG`) |
| scrap_qty_pc | BIGINT | scrap in pieces (KG / net weight, rounded) |
| reason_code | STRING | scrap reason |
| recorded_at | TIMESTAMP | booking time |
| is_late_arrival | BOOLEAN | booked more than 48 h after the event |

### silver_inspections (key `inspection_id`)
| Column | Type | Description |
|---|---|---|
| inspection_id | STRING | inspection lot number |
| run_id | STRING | inspected machine run |
| material_id | STRING | inspected material |
| inspected_at | TIMESTAMP | inspection time |
| inspector | STRING | inspector id |
| sample_size | INT | pieces inspected |
| defects_found | INT | defective pieces in the sample |
| result | STRING | `PASS` or `FAIL` |
| recorded_at | TIMESTAMP | booking time in the QMS |
| is_late_arrival | BOOLEAN | booked more than 48 h after the inspection |

### silver_complaints (key `complaint_id`)
| Column | Type | Description |
|---|---|---|
| complaint_id | STRING | complaint number |
| order_id, order_line | STRING, INT | complained order line |
| material_id | STRING | complained material |
| received_at | TIMESTAMP | complaint received |
| category | STRING | Dimension, Surface, Function, Packaging, Delivery |
| severity | STRING | minor, major, critical |
| qty_affected | INT | affected pieces |
| status | STRING | open or closed |

### silver_quarantine
| Column | Type | Description |
|---|---|---|
| entity | STRING | source entity, e.g. `orders` |
| business_key | STRING | key values joined with `\|`, `<null>` for missing parts |
| dq_reason | STRING | first failing rule: `missing_key`, `orphan_fk`, `unknown_unit`, `out_of_range`, `future_date`, `invalid_value` |
| record_json | STRING | the typed record as JSON |
| batch_id | STRING | pipeline run id |
| quarantined_at | TIMESTAMP | load time |

### silver_load_stats
| Column | Type | Description |
|---|---|---|
| entity | STRING | source entity |
| bronze_rows | BIGINT | rows read from bronze |
| quarantined | BIGINT | rows routed to quarantine |
| duplicates_removed | BIGINT | rows dropped by deduplication |
| silver_rows | BIGINT | rows written to silver |
| late_flagged | BIGINT | rows with `is_late_arrival` |
| units_converted | BIGINT | rows whose unit was converted |

## Gold

### dim_date (key `date`)
| Column | Type | Description |
|---|---|---|
| date | DATE | calendar day, marked as the date table |
| date_key | INT | `yyyyMMdd` |
| year | INT | calendar year |
| quarter | INT | 1 to 4 |
| month | INT | 1 to 12, sort key of `month_name` |
| month_name | STRING | `Jan` .. `Dec` |
| year_month | STRING | `2026-04` |
| iso_week | INT | ISO week |
| day_of_week | INT | 1 = Sunday .. 7 = Saturday (Spark convention) |
| is_weekend | BOOLEAN | Saturday or Sunday |

### dim_material (key `material_id`)
Columns `material_id`, `material_name`, `material_type`, `material_group`, `base_unit`,
`net_weight_kg`, `standard_cost_eur`, `list_price_eur`, `supplier_id`, as in silver.

### dim_supplier (key `supplier_id`)
Columns `supplier_id`, `supplier_name`, `country_code`, `rating`, `lead_time_days`, as in silver.

### dim_machine (key `machine_id`)
Columns `machine_id`, `machine_name`, `machine_type`, `line`, `plant`, `ideal_cycle_time_s`,
`commissioned_year`, as in silver.

### fact_orders (grain: order line)
| Column | Type | Description |
|---|---|---|
| order_id | STRING | sales order |
| order_line | INT | line |
| customer_id | STRING | customer |
| material_id | STRING | to `dim_material` |
| plant | STRING | plant |
| order_date | DATE | to `dim_date` |
| requested_date | DATE | requested delivery |
| delivered_date | DATE | actual delivery, NULL if open |
| quantity_pc | BIGINT | pieces |
| net_value_eur | DOUBLE | net value |
| is_delivered | BOOLEAN | delivered by the as-of date |
| is_on_time | BOOLEAN | delivered on or before requested date, NULL if open |
| delivery_delay_days | INT | delivered minus requested, NULL if open |
| is_late_arrival | BOOLEAN | from silver |

### fact_production (grain: machine run)
| Column | Type | Description |
|---|---|---|
| run_id | STRING | machine run |
| machine_id | STRING | to `dim_machine` |
| material_id | STRING | to `dim_material` |
| shift | STRING | shift |
| run_date | DATE | to `dim_date` |
| planned_runtime_min | DOUBLE | planned time |
| downtime_min | DOUBLE | downtime |
| run_time_min | DOUBLE | planned minus downtime |
| ideal_cycle_time_s | INT | from the machine master |
| ideal_run_time_min | DOUBLE | `units_produced x ideal_cycle_time_s / 60` |
| units_produced | BIGINT | units produced |
| scrap_units | BIGINT | scrap pieces booked against the run |
| good_units | BIGINT | units produced minus scrap |
| is_late_arrival | BOOLEAN | from silver |

### fact_inspections (grain: inspection)
| Column | Type | Description |
|---|---|---|
| inspection_id | STRING | inspection |
| run_id | STRING | inspected run |
| machine_id | STRING | to `dim_machine` (via the run) |
| material_id | STRING | to `dim_material` |
| inspection_date | DATE | to `dim_date` |
| sample_size | INT | pieces inspected |
| defects_found | INT | defective pieces |
| result | STRING | PASS or FAIL |
| is_pass | BOOLEAN | result is PASS |

### fact_complaints (grain: complaint)
| Column | Type | Description |
|---|---|---|
| complaint_id | STRING | complaint |
| order_id | STRING | complained order |
| order_line | INT | complained line |
| material_id | STRING | to `dim_material` |
| complaint_date | DATE | to `dim_date` |
| category | STRING | complaint category |
| severity | STRING | severity |
| status | STRING | open or closed |
| qty_affected | INT | affected pieces |

### gold_kpi_summary (grain: KPI)
| Column | Type | Description |
|---|---|---|
| kpi_name | STRING | identical to the DAX measure name |
| kpi_value | DOUBLE | value over the whole period, ratios as fractions |
| kpi_area | STRING | Delivery, Production, OEE, Quality |
| value_format | STRING | the measure's format string |

### gold_kpi_monthly (grain: month)
| Column | Type | Description |
|---|---|---|
| year_month | STRING | `2026-04` |
| order_lines, delivered_lines, on_time_lines | BIGINT | order counts by order month |
| on_time_delivery_pct | DOUBLE | on-time lines / delivered lines |
| units_produced, scrap_units | BIGINT | by run month |
| scrap_rate_pct | DOUBLE | scrap / produced |
| availability_pct, performance_pct, quality_rate_pct, oee_pct | DOUBLE | OEE factors by run month |
| complaints | BIGINT | by complaint month |
| complaint_rate_pct | DOUBLE | complaints / delivered lines of the month |
