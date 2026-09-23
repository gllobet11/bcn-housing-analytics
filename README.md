# bcn-housing-analytics

Pipeline ELT sobre datos abiertos de Barcelona (Inside Airbnb + alquiler medio Open Data BCN). Modelo dimensional en dbt + ClickHouse. Ver `kickoff.md` para el plan completo y `progress.md` para el registro de avance.

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

## Calidad de código

```bash
pre-commit run --all-files
```

Ejecuta `ruff` sobre Python y `sqlfluff` (dialecto `clickhouse`, templater `dbt`) sobre los modelos SQL.
