# KPI definitions

All fifteen KPIs exist twice: as a DAX measure in the semantic model
(`powerbi/ManufacturingKPIs.SemanticModel/definition/tables/*.tmdl`) and as a row of
`gold_kpi_summary`, computed by Spark SQL in `nb_03_gold_kpis`. The test
`tests/test_lakehouse.py::test_dax_measures_reproduce_spark_sql_kpis` evaluates every DAX
measure against the gold tables and requires the same value as Spark SQL.

Percentages are stored as fractions (0.873) and formatted by the measure's `formatString`.

## Delivery (home table `fact_orders`)

| Measure | Definition | DAX |
|---|---|---|
| Order Lines | order lines after cleaning | `COUNTROWS ( fact_orders )` |
| Net Sales EUR | sum of net order value | `SUM ( fact_orders[net_value_eur] )` |
| Delivered Lines | lines with a delivery date | `CALCULATE ( COUNTROWS ( fact_orders ), fact_orders[is_delivered] = TRUE () )` |
| On-Time Lines | delivered lines with `delivered_date <= requested_date` | `CALCULATE ( COUNTROWS ( fact_orders ), fact_orders[is_on_time] = TRUE () )` |
| On-Time Delivery % | On-Time Lines / Delivered Lines | `DIVIDE ( [On-Time Lines], [Delivered Lines] )` |

Open lines (not delivered by the as-of date) are excluded from the on-time denominator. A line
delivered on the requested day counts as on time. Filtering by date uses `order_date`.

## Production and OEE (home table `fact_production`)

| Measure | Definition | DAX |
|---|---|---|
| Units Produced | units produced over all runs | `SUM ( fact_production[units_produced] )` |
| Scrap Units | scrapped units, KG bookings converted to pieces | `SUM ( fact_production[scrap_units] )` |
| Scrap Rate % | Scrap Units / Units Produced | `DIVIDE ( [Scrap Units], [Units Produced] )` |
| Availability % | run time / planned production time | `DIVIDE ( SUM ( fact_production[run_time_min] ), SUM ( fact_production[planned_runtime_min] ) )` |
| Performance % | ideal run time / run time, ideal run time = units produced x ideal cycle time | `DIVIDE ( SUM ( fact_production[ideal_run_time_min] ), SUM ( fact_production[run_time_min] ) )` |
| Quality Rate % | good units / units produced | `DIVIDE ( SUM ( fact_production[good_units] ), [Units Produced] )` |
| OEE % | Availability x Performance x Quality Rate | `VAR` per factor, `RETURN _availability * _performance * _quality` |

* `run_time_min = planned_runtime_min - downtime_min`, all in minutes after silver converted
  runs booked in hours.
* `ideal_cycle_time_s` comes from the machine master; the fact table carries it per run so the
  product `units_produced x ideal_cycle_time_s / 60` is exact per run before summing.
* Ratios are ratios of sums, not averages of per run ratios, so long runs weigh more, as in the
  usual OEE roll up.
* OEE here is "OEE-style": planned time is the shift time booked by the MES, not calendar time,
  and quality counts scrap only (no rework category in the source).

## Quality (home tables `fact_complaints`, `fact_inspections`)

| Measure | Definition | DAX |
|---|---|---|
| Complaints | customer complaints received | `COUNTROWS ( fact_complaints )` |
| Complaint Rate % | Complaints / Delivered Lines | `DIVIDE ( [Complaints], [Delivered Lines] )` |
| Inspection Pass Rate % | inspections with result PASS / all inspections | `DIVIDE ( CALCULATE ( COUNTROWS ( fact_inspections ), fact_inspections[is_pass] = TRUE () ), COUNTROWS ( fact_inspections ) )` |

The complaint rate divides complaints by delivered order lines. Under a date filter the
numerator follows `complaint_date` and the denominator `order_date`, which is the usual
compromise for a monthly complaint ratio; a cohort view (complaints of the lines ordered in a
month) would need a relationship from `fact_complaints` to `fact_orders`, which the star schema
avoids on purpose.

## Monthly view

`gold_kpi_monthly` carries the same ratios per `year_month` (orders by order month, production
by run month, complaints by complaint month) for consumers that do not use the semantic model.
