.PHONY: install test inspect-sample

install:
	python -m pip install -e ".[dev]"

test:
	pytest

inspect-sample:
	python scripts/inspect_data.py --input data/sample/transactions_sample.csv
