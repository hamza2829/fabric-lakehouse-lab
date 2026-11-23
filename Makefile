PY ?= python

.PHONY: all validate data pipeline quality test results clean

all: validate data pipeline quality test

validate:
	$(PY) pipelines/validate.py

data:
	$(PY) data/generate_data.py --seed 42

pipeline:
	$(PY) pipelines/run.py pl_lakehouse_daily

quality:
	$(PY) -m quality.run_quality

test:
	$(PY) -m pytest

results:
	$(PY) tools/render_results.py --write

clean:
	rm -rf data/raw onelake reports spark-warehouse metastore_db derby.log
