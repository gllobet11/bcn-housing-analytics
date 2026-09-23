# progress.md — bcn-housing-analytics

Registro por fase: qué se hizo, decisiones tomadas (y por qué), errores encontrados y cómo se resolvieron.

## Fase 0 — Setup
- Estado: hecho
- Qué se hizo: `docker-compose.yml` (ClickHouse 24.8), proyecto dbt `housing` (models/seeds/snapshots/analyses/tests/macros), `profiles.yml` en la raíz leyendo credenciales de env vars (`CLICKHOUSE_HOST/PORT/USER/PASSWORD/SCHEMA`), `requirements.txt`, `.pre-commit-config.yaml` (ruff + sqlfluff dialect clickhouse/templater dbt), `.sqlfluff`, `.env.example`, `.gitignore`, README.
- Decisiones:
  - `profiles.yml` vive en la raíz del repo (no en `~/.dbt/`) y se referencia con `DBT_PROFILES_DIR=$(pwd)`, para que el proyecto sea autocontenido y reproducible en CI.
  - `secure: false` en el perfil porque ClickHouse corre local sin TLS.
- Errores encontrados:
  - Con `CLICKHOUSE_PASSWORD` vacío, el entrypoint oficial de ClickHouse deshabilita el acceso por red del usuario `default` ("neither CLICKHOUSE_USER nor CLICKHOUSE_PASSWORD is set"), y `dbt debug` fallaba con `AUTHENTICATION_FAILED`. Solución: poner una contraseña de desarrollo no vacía en `.env.example` (`dev_password`).
- Validado: `docker compose up -d` levanta ClickHouse (healthy); `dbt debug` → "All checks passed!"; `pre-commit run --all-files` se ejecuta sin errores (sin ficheros que lintar todavía, esperado en esta fase).

## Fase 1 — Ingesta EL
- Estado: pendiente

## Fase 2 — Staging + tests
- Estado: pendiente

## Fase 3 — Modelado dimensional
- Estado: pendiente

## Fase 4 — Incremental + SCD2
- Estado: pendiente

## Fase 5 — Airflow
- Estado: pendiente

## Fase 6 — CI/CD
- Estado: pendiente

## Fase 7 — Nuevo mercado + análisis
- Estado: pendiente
