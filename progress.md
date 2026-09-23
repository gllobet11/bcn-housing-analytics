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
- Estado: hecho
- Qué se hizo: `ingestion/load_raw.py` descarga los 4 snapshots trimestrales de Inside Airbnb Barcelona (listings, calendar, neighbourhoods) y las estadísticas `b37xv8wcjh` (€/mes) y `5ibudgqbrb` (€/m²) de la API no documentada de Barcelona Dades, y lo carga tal cual en `raw` de ClickHouse (todas las columnas fuente como `Nullable(String)`, más `_snapshot_date` y `_loaded_at`). El JSON crudo de Barcelona Dades se guarda en `data/landing/bcn_rent/` antes de aplanar. `models/sources.yml` declara las 4 fuentes con `loaded_at_field: _loaded_at` y umbral de freshness (100/130 días para Airbnb, 7/30 para bcn_rent, que refleja la cadencia de *extracción*, no la del dato).
- Decisiones:
  - **Fuente de alquiler cambiada respecto al kickoff original**: el dataset `est-mercat-immobiliari-lloguer-mitja-mensual` de Open Data BCN (CKAN) ya no existe — devuelve 404, y recorrer el catálogo completo (555 datasets) no encuentra nada de alquiler por barrio. La fuente real es una API REST no documentada de `portaldades.ajuntament.barcelona.cat` (`/services/backend/rest/statistic?id=<id>&language=ca`, header `X-IBM-Client-Id` leído en runtime desde `/portal/config/apimanagerconsumercredentials.json`). Verificado y confirmado funcionando el 2026-09-23. Detalle completo del hallazgo ya está en `kickoff.md`.
  - Idempotencia vía `DROP PARTITION` + `INSERT` (no `ReplacingMergeTree`): con Replacing, el conteo justo después de recargar podía no reflejar la deduplicación hasta el merge en background, lo que rompía la validación "recargar no duplica". Partición = `(_snapshot_date)` para Airbnb, `(stat_id, _snapshot_date)` para `bcn_rent` (dos estadísticas comparten tabla y snapshot_date, así que partición solo por fecha borraba la otra estadística al recargar — bug encontrado y corregido durante la implementación).
  - `_snapshot_date` en `bcn_rent` es la fecha de la *extracción* (la API devuelve toda la serie histórica en cada llamada), no un periodo del dato — el periodo real vive en `period_id`/`period_type` (fila aplanada de `dims[0]`).
  - Test de contrato (`_validate_rent_contract`) falla explícito si la respuesta de Barcelona Dades pierde las claves `dims`/`values`/`dim0`/`dim1`/`value`, dado que es una API interna sin garantías de estabilidad.
  - Esquema de columnas de ClickHouse creado dinámicamente desde el header de cada CSV/JSON fuente (capa raw sin tipar a propósito; el tipado es cosa de la Fase 2). Al añadir una columna nueva en un snapshot (p. ej. Inside Airbnb añadió `price` al calendar en algún trimestre) se hace `ALTER TABLE ADD COLUMN IF NOT EXISTS` antes de insertar.
- Errores encontrados:
  - `clickhouse-connect` no serializa un `str` en una columna `Date`: hay que pasar `date.fromisoformat(...)`.
  - `PARTITION BY` con columnas `Nullable(String)` falla (`allow_nullable_key` desactivado) — se envuelve en `ifNull(col, '')` en la expresión de partición.
  - Bug de partición compartida en `bcn_rent` (ver decisiones) detectado al comprobar el conteo por `stat_id` tras la primera carga: solo aparecía uno de los dos stat_id.
  - El calendar de Inside Airbnb tiene campos de texto libre con saltos de línea entre comillas; `wc -l` sobre el CSV no es fiable para contar filas (hay que usar un parser CSV real) — usado para validar conteos.
- Validado: los 4 snapshots de Airbnb y las 2 estadísticas de alquiler cargados; conteos por snapshot cuadran con lo que da un parser CSV/JSON real sobre los ficheros descargados; ejecutar el script dos veces seguidas da exactamente los mismos conteos por partición (sin duplicar); `dbt source freshness` pasa en verde para las 4 fuentes; `pre-commit run --all-files` en verde (`ruff-format` reformateó el script automáticamente).

## Fase 2 — Staging + tests
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
