# Synthetic source data

`python data/generate_data.py --seed 42` writes `data/raw/` (git ignored):

| Source | File | Format | Rows (seed 42) |
|---|---|---|---:|
| ERP | `erp/suppliers.csv` | CSV | 43 |
| ERP | `erp/materials.csv` | CSV | 129 |
| ERP | `erp/orders.csv` | CSV | 5,844 |
| Production | `production/machines.csv` | CSV | 12 |
| Production | `production/machine_runs.csv` | CSV | 3,697 |
| Production | `production/scrap_events.jsonl` | JSON lines | 4,393 |
| Quality | `quality/inspections.jsonl` | JSON lines | 2,919 |
| Quality | `quality/complaints.json` | JSON array | 107 |

Each table is generated clean, then defects are injected on disjoint rows, so every bad row has
exactly one defect:

| Defect | Where | Expected handling |
|---|---|---|
| exact or versioned duplicates | all transactional tables, suppliers, materials | removed, newest extract wins |
| missing keys | orders, materials, runs, scrap, inspections, complaints | quarantined |
| orphan foreign keys | orders, materials, runs, scrap, inspections, complaints | quarantined |
| unit mismatches | order units `PCS`/`pc`/`Stk`/`DZ`, run times in hours, scrap in KG | converted |
| out of range values | negative quantities, downtime above plan, defects above sample | quarantined |
| future dated records | orders, runs | quarantined |
| late records | orders, runs, scrap, inspections | kept and flagged |
| stale feed | complaints stop 4 days before the as-of date | freshness warning |

`data/raw/_manifest.json` records the rows written, the expected silver rows, the injected
counts per defect and a SHA-256 per file. The same seed always produces byte identical files.
