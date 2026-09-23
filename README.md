# bcn-housing-analytics

Pipeline ELT sobre datos abiertos de Barcelona (Inside Airbnb + alquiler medio por barrio de Barcelona Dades). Modelo dimensional en dbt + ClickHouse. Ver `kickoff.md` para el plan completo y `progress.md` para el registro de avance.

## Requisitos

- Docker + Docker Compose
- Python 3.11+

## Arranque

```bash
cp .env.example .env
docker compose up -d
docker compose ps   # esperar a que clickhouse esté "healthy"

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

pre-commit install

set -a && source .env && set +a
export DBT_PROFILES_DIR=$(pwd)
cd housing
dbt debug
```

`dbt debug` debe terminar con "All checks passed!".

## Orquestación con Airflow

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up -d airflow-postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

UI en http://localhost:8080 (usuario/contraseña `admin`/`admin`). El DAG `housing_pipeline` corre `ingest_raw` → los modelos dbt (una task por modelo vía Cosmos, con sus tests) → `dbt source freshness`, con `schedule="@quarterly"` y `catchup=False`.

## Calidad de código

```bash
pre-commit run --all-files
```

Ejecuta `ruff` sobre Python y `sqlfluff` (dialecto `clickhouse`, templater `dbt`) sobre los modelos SQL.

## CI/CD

- `ci.yml` (en cada PR): pre-commit, `dbt build` completo contra fixtures (~200 filas/fuente, perfil `ci`) en un ClickHouse de un solo uso, y comprobación de que el DAG de Airflow importa sin errores.
- `cd.yml` (al hacer merge a `main`): genera los docs de dbt y los publica en GitHub Pages.

Docs publicados: https://<usuario>.github.io/bcn-housing-analytics/ (placeholder hasta el primer merge a `main` con el repo en GitHub).
