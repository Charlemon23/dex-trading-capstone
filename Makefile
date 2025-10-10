.PHONY: venv install run once

venv:
	python -m venv .venv

install: venv
	. .venv/bin/activate && pip install -r requirements.txt

once:
	. .venv/bin/activate && python scripts/collect_dex_data.py

run:
	. .venv/bin/activate && python scripts/collect_dex_data.py --interval 300
