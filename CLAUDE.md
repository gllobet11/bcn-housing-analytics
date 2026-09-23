# bcn-housing-analytics

Proyecto de portfolio: pipeline ELT sobre datos abiertos de Barcelona (Inside Airbnb + alquiler medio por barrio de la API de Barcelona Dades) con dbt-core + ClickHouse, orquestado con Airflow, CI/CD en GitHub Actions. Ver `kickoff.md` para el plan completo por fases y `progress.md` para el registro de avance.

## Forma de trabajo
- Se implementa **una fase a la vez**, siguiendo el prompt de esa fase en `kickoff.md`.
- Al terminar y validar una fase, actualizar `progress.md`: estado, decisiones tomadas (y por qué), errores encontrados y cómo se resolvieron.
- No adelantar trabajo de fases futuras (p. ej. no meter Airflow antes de la Fase 5, no meter Madrid antes de la Fase 7).
- Coste 0 €: todo local/Docker salvo GitHub (repo público → Actions y Pages gratis).

## Stack
- Warehouse: ClickHouse (Docker)
- Transformación: dbt-core + dbt-clickhouse
- Orquestación: Airflow + astronomer-cosmos (desde Fase 5)
- CI/CD: GitHub Actions + GitHub Pages (desde Fase 6)
- Calidad: sqlfluff (dialect clickhouse, templater dbt), ruff, pre-commit

## Estructura objetivo
```
raw → staging → intermediate → marts (dim_*, fct_*, mart_barrio_quarter)
```
Grano de cada modelo documentado en `schema.yml`. Detalle completo del modelo dimensional en `kickoff.md`.
