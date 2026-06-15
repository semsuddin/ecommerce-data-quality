.PHONY: setup test gen validate spark local clean

setup:
	pip install -r requirements.txt -r requirements-dev.txt

test:
	pytest -q

gen:
	python generate_data.py --rows 5000 --out data/raw

validate:
	cd validation && python run_checks.py --engine duckdb --data-dir ../data/raw

spark:
	cd spark && python daily_revenue_agg.py --source local --data-dir ../data/raw --out ../data/agg

# full local pipeline, no cloud account required
local: gen validate spark

clean:
	rm -rf data target dbt_packages logs __pycache__ */__pycache__ .pytest_cache
