"""DAG housing_pipeline: ingesta EL -> dbt (Cosmos, una task por modelo) -> source freshness.

Corre trimestralmente porque las fuentes (Inside Airbnb, Barcelona Dades) publican
snapshots trimestrales; `catchup=False` porque solo interesa el estado actual, no
rellenar histórico al desplegar el DAG.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, RenderConfig

log = logging.getLogger(__name__)

PROJECT_DIR = "/opt/airflow/project"
DBT_PROJECT_DIR = f"{PROJECT_DIR}/housing"
PROFILES_YML = f"{PROJECT_DIR}/profiles.yml"


def log_failure(context: dict) -> None:
    task_instance = context["task_instance"]
    log.error(
        "housing_pipeline task failed: dag=%s task=%s run_id=%s exception=%s",
        context["dag"].dag_id,
        task_instance.task_id,
        context["run_id"],
        context.get("exception"),
    )


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": log_failure,
}

profile_config = ProfileConfig(
    profile_name="housing",
    target_name="dev",
    profiles_yml_filepath=PROFILES_YML,
)


@dag(
    dag_id="housing_pipeline",
    description="Ingesta EL + dbt (Cosmos) + source freshness sobre bcn-housing-analytics",
    schedule="@quarterly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["housing", "dbt"],
)
def housing_pipeline():
    ingest = BashOperator(
        task_id="ingest_raw",
        bash_command=f"cd {PROJECT_DIR} && python ingestion/load_raw.py",
    )

    dbt_models = DbtTaskGroup(
        group_id="dbt_housing",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        render_config=RenderConfig(select=["path:models"]),
    )

    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt source freshness --profiles-dir {PROJECT_DIR}",
    )

    ingest >> dbt_models >> freshness


housing_pipeline()
