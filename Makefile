.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

test: ## Run DAG integrity tests
	pytest -q

lint: ## Ruff lint + format check (same as CI)
	ruff check dags tests && ruff format --check dags tests

fmt: ## Auto-fix lint and formatting
	ruff check --fix dags tests && ruff format dags tests

deploy: ## Copy DAGs into the running Airflow container
	docker cp dags/. meridian-airflow-scheduler:/opt/airflow/dags/
	@echo "DAGs deployed. The scheduler picks them up within ~30s."

pool: ## Create the warehouse pool that limits concurrent writes
	docker exec meridian-airflow-scheduler \
		airflow pools set warehouse_pool 2 "Limits concurrent warehouse writes"

list-dags: ## List DAGs as Airflow sees them
	docker exec meridian-airflow-scheduler airflow dags list

trigger-eod: ## Manually trigger the EOD pipeline for today
	docker exec meridian-airflow-scheduler airflow dags trigger eod_pipeline
