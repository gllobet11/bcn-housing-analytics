# bcn-housing-analytics

Pipeline ELT sobre datos abiertos de Barcelona: oferta de Airbnb, licencias de vivienda turística, viviendas turísticas anunciadas y alquiler medio. Modelo dimensional en dbt + ClickHouse, orquestado con Airflow, CI/CD en GitHub Actions y un notebook de análisis con mapa interactivo. Ver `kickoff.md` para el plan completo y `progress.md` para el registro de avance (decisiones y errores de cada fase).

## Fuentes

| Fuente | Qué aporta | Histórico | `load_raw.py --only` |
|---|---|---|---|
| [Inside Airbnb](https://insideairbnb.com/get-the-data/) (Barcelona) | Anuncios, calendario y barrios | 4 snapshots trimestrales (sep-2025 → jun-2026); lo anterior, por [solicitud](https://insideairbnb.com/data-requests/) | `airbnb` |
| [Barcelona Dades](https://portaldades.ajuntament.barcelona.cat) | Alquiler medio €/mes y €/m² por barrio y distrito | 2014 → hoy (barrio), 2000 → hoy (distrito) | `rent` |
| [Open Data BCN, habitatges d'ús turístic](https://opendata-ajuntament.barcelona.cat/data/es/dataset/habitatges-us-turistic) | Registro municipal de licencias HUT (nº HUTB, barrio, plazas, coordenadas) | 2018T2 → hoy (nº HUTB desde 2020T3) | `hut` |
| [INE, viviendas turísticas](https://www.ine.es/experimental/viv_turistica/experimental_viv_turistica.htm) | Viviendas turísticas anunciadas (Airbnb + Booking + Vrbo) por distrito | 2020-08 → hoy, semestral | `ine` |

`load_raw.py` sin argumentos carga todo (es lo que ejecuta el DAG). Es idempotente por partición. Los ficheros descargados se cachean en `data/landing/` (no versionado); el histórico del registro municipal trae CSV mal formados que se reparan al leerlos (ver `progress.md`).

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

## Carga y construcción manual

```bash
set -a && source .env && set +a
export DBT_PROFILES_DIR=$(pwd)
python ingestion/load_raw.py          # todas las fuentes (o --only airbnb|rent|hut|ine)
cd housing && dbt deps && dbt build
```

Historial SCD2 de anuncios (`snp_listings` → `fct_price_changes`): `dbt snapshot` toma el snapshot de Airbnb más reciente. Para reconstruirlo desde cero, vaciar `snp_listings` y ejecutar un snapshot por fecha, en orden cronológico:

```bash
for d in 2025-09-14 2025-12-14 2026-03-21 2026-06-24; do dbt snapshot --vars "{snapshot_date: '$d'}"; done
```

## Orquestación con Airflow

```bash
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up -d airflow-postgres airflow-init
docker compose up -d airflow-webserver airflow-scheduler
```

UI en http://localhost:8080 (usuario/contraseña `admin`/`admin`). El DAG `housing_pipeline` corre `ingest_raw` → los modelos dbt (una task por modelo vía Cosmos, con sus tests) → `dbt source freshness`, con `schedule="@quarterly"` y `catchup=False`.

## Análisis

```bash
pip install -r requirements-analysis.txt   # pandas, folium, matplotlib, jupyterlab
jupyter lab notebooks/analisis_licencias.ipynb
```

El notebook lee de los marts (necesita ClickHouse levantado y `dbt build` hecho) y cubre: evolución de la oferta de Airbnb, clasificación de licencias cruzada con el registro municipal, alquiler de temporada, HUTB compartidos, evolución macro 2014-2026 indexada, cruce por barrio y por distrito, conclusiones y recomendaciones.

El mapa interactivo (un punto por anuncio con sus condiciones, capas por situación de licencia y coropletas de €/m² por barrio y distrito) se guarda en `notebooks/output/mapa_licencias.html` (no versionado; ~11 MB). En el notebook versionado su salida está omitida.

## Calidad de código

```bash
pre-commit run --all-files
```

Ejecuta `ruff` sobre Python y `sqlfluff` (dialecto `clickhouse`, templater `dbt`) sobre los modelos SQL.

## CI/CD

- `ci.yml` (en cada PR): pre-commit, `dbt build` completo contra fixtures (~200 filas/fuente, perfil `ci`) en un ClickHouse de un solo uso, y comprobación de que el DAG de Airflow importa sin errores.
- `cd.yml` (al hacer merge a `main`): genera los docs de dbt y los publica en GitHub Pages.

Docs publicados: https://gllobet11.github.io/bcn-housing-analytics/
