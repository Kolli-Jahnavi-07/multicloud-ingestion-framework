PY ?= python
export PYTHONPATH := src

.PHONY: install test lint demo tf-check

install:
	$(PY) -m pip install -r requirements-dev.txt

test:
	$(PY) -m pytest -q

lint:
	$(PY) -m ruff check src tests demo
	$(PY) -m ruff format --check src tests demo

demo:
	$(PY) -m ingest demo

tf-check:
	terraform -chdir=infra/terraform/aws fmt -check
	terraform -chdir=infra/terraform/azure fmt -check
	terraform -chdir=infra/terraform/aws init -backend=false -input=false >/dev/null && terraform -chdir=infra/terraform/aws validate
	terraform -chdir=infra/terraform/azure init -backend=false -input=false >/dev/null && terraform -chdir=infra/terraform/azure validate
