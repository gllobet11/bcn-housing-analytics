# bcn-housing-analytics — kickoff

Proyecto de portfolio para demostrar **data modeling (dbt + ClickHouse), orquestación (Airflow) y CI/CD (GitHub Actions)** sobre datos inmobiliarios reales de Barcelona. Todo gratis y en local, salvo GitHub (repo público → Actions y Pages gratis).

Objetivo de negocio (lo que contarás en la entrevista):
> "¿En qué barrios de Barcelona el alquiler turístico presiona más el precio del alquiler residencial, y cómo evoluciona trimestre a trimestre?"

Forma de trabajo: un prompt por fase en Claude Code, `/clear` entre fases, validar los criterios antes de pasar a la siguiente y apuntar decisiones y errores en `progress.md`.

---

## Stack (coste 0 €)

| Pieza | Herramienta | Notas |
|---|---|---|
| Warehouse | ClickHouse (Docker `clickhouse/clickhouse-server`) | El mismo motor que usa idealista |
| Transformación | dbt-core + adaptador `dbt-clickhouse` | Open source |
| Orquestación | Airflow (Docker Compose oficial o `astro dev`) + `astronomer-cosmos` | Cosmos convierte cada modelo dbt en una task de Airflow. Airflow necesita ~4 GB de RAM para Docker |
| Object storage (opcional) | MinIO (compatible con S3) | Para imitar la capa raw en S3 |
| CI/CD | GitHub Actions + GitHub Pages | Gratis en repos públicos |
| Calidad de código | sqlfluff (dialecto clickhouse + templater dbt), ruff, pre-commit | |

## Fuentes de datos (licencia abierta)

1. **Inside Airbnb, Barcelona**: `listings.csv.gz`, `calendar.csv.gz` y `neighbourhoods.geojson`. Licencia CC BY 4.0. Hay descarga libre de los snapshots trimestrales del último año; el último es del 24/06/2026. Con 4 snapshots tienes material para modelos incrementales y SCD2.
2. **Open Data BCN, alquiler medio por barrio**: datasets `est-mercat-immobiliari-lloguer-mitja-mensual` (€/mes) y `...-lloguer-superficie-mitjana` (€/m²). Son trimestrales por barrio y vienen de las fianzas del Incasòl.
3. **Mercado adicional (fase 7)**: Inside Airbnb de Madrid o Lisboa. Así demuestras el "adaptar a nuevos mercados" que pide la oferta.

> Problema real de data quality incluido: los nombres de barrio de Inside Airbnb y los códigos de Open Data BCN no coinciden 1:1. Hay que resolverlo con un seed de mapeo testeado.

## Modelo objetivo

```
raw (tal cual)  →  staging (tipado/limpieza)  →  intermediate  →  marts
                                                                 ├─ dim_geography   (ciudad › distrito › barrio)
                                                                 ├─ dim_property_type
                                                                 ├─ dim_date
                                                                 ├─ fct_listing_snapshot   grano: listing × fecha_snapshot
                                                                 ├─ fct_rent_quarter       grano: barrio × trimestre
                                                                 └─ mart_barrio_quarter    grano: barrio × trimestre (KPIs para negocio)
snapshots: snp_listings (SCD2 sobre precio, room_type y availability)
```

KPIs de `mart_barrio_quarter`: número de anuncios activos, % de alojamiento entero, precio medio por noche, alquiler medio €/mes y €/m², y variación trimestral de cada uno.

---

## Fase 0 — Setup (~2 h)

**Prompt:**
> Crea el esqueleto del repo `bcn-housing-analytics`: `docker-compose.yml` con ClickHouse (puertos 8123/9000, volumen persistente), proyecto dbt `housing` con perfil dbt-clickhouse que lea las credenciales de variables de entorno, `requirements.txt` (dbt-clickhouse, sqlfluff, ruff, pre-commit), `.pre-commit-config.yaml` con sqlfluff (dialect clickhouse, templater dbt) y ruff, `.env.example` y un README con cómo arrancar. Aún no metas Airflow.

**Validar:** `docker compose up -d` arranca; `dbt debug` pasa en verde; `pre-commit run --all-files` se ejecuta.

## Fase 1 — Ingesta EL (~3 h)

**Prompt:**
> Script Python `ingestion/load_raw.py` que descarga Inside Airbnb Barcelona (listings, calendar y neighbourhoods de los snapshots disponibles) y los CSV de alquiler medio por barrio de Open Data BCN, y los carga sin transformar en la base `raw` de ClickHouse. Añade una columna `_snapshot_date` y otra `_loaded_at`. Debe ser idempotente: si se recarga un snapshot ya cargado, se reemplaza y no se duplica. Declara las fuentes en `models/sources.yml` con `loaded_at_field` y un umbral de freshness.

**Validar:** los conteos de filas por snapshot cuadran con los ficheros; si cargas dos veces, el conteo no cambia; `dbt source freshness` funciona.

## Fase 2 — Staging + tests (~3 h)

**Prompt:**
> Modelos `stg_airbnb__listings`, `stg_airbnb__calendar` y `stg_bcn__rent` como vistas. Hay que tipar, renombrar a snake_case y limpiar el precio (el texto "$1,234.00" pasa a Decimal). Crea un seed `seed_barrio_mapping.csv` que mapee los barrios de Inside Airbnb a los códigos de Open Data BCN. Tests: `unique` y `not_null` en las claves, `accepted_values` en room_type, `relationships` entre listings y el mapping, y un test propio que falle si más del 1 % de los anuncios no tiene barrio mapeado.

**Validar:** `dbt build --select staging` pasa en verde. Rompe a propósito una fila del seed y comprueba que el test falla.

## Fase 3 — Modelado dimensional (~4 h) ← núcleo del "data modeling"

**Prompt:**
> Construye las dimensiones y hechos del diagrama del kickoff. Documenta en `schema.yml` el grano de cada tabla. Para ClickHouse, configura en cada modelo `engine`, `order_by` y `partition_by` según cómo se va a consultar (por ejemplo, `fct_listing_snapshot` con ORDER BY (barrio_id, snapshot_date, listing_id)) y justifícalo en un comentario. Añade tests de grano con `dbt_utils.unique_combination_of_columns`. Por último, crea `mart_barrio_quarter` con los KPIs del kickoff.

**Validar:** los tests de grano pasan; `dbt docs generate` muestra el lineage completo; una query sobre el mart responde en menos de 1 s.

## Fase 4 — Incremental + SCD2 (~3 h)

**Prompt:**
> Convierte `fct_listing_snapshot` en un modelo incremental (elige la estrategia de dbt-clickhouse más adecuada y justifícala) que procese solo los snapshots nuevos. Crea `snp_listings` (snapshot dbt con estrategia `check` sobre price, room_type y availability_365). Añade un modelo `fct_price_changes` que salga del snapshot.

**Validar:** carga los snapshots de uno en uno y ejecuta dbt después de cada uno: el resultado final debe ser idéntico al de un `--full-refresh`. Ejecutarlo dos veces seguidas no duplica filas.

## Fase 5 — Orquestación con Airflow (~3 h)

**Prompt:**
> Añade Airflow al docker-compose (o usa `astro dev`) y un DAG `housing_pipeline` con: task de ingesta, luego el proyecto dbt renderizado con Cosmos (una task por modelo, con sus tests), luego `dbt source freshness`. Configura retries con backoff, un `on_failure_callback` que registre el error y `schedule` trimestral con `catchup=False`.

**Validar:** el DAG termina en verde en la UI; si metes un fallo a propósito en un modelo, solo fallan esa task y las que dependen de ella.

## Fase 6 — CI/CD con GitHub Actions (~3 h) ← núcleo del "CI/CD"

**Prompt:**
> Crea dos workflows.
> 1. `ci.yml`, que se ejecuta en cada pull request: pre-commit (sqlfluff + ruff); ClickHouse como service container; carga de fixtures pequeños (seeds con unas 200 filas por fuente, en un perfil `ci`); `dbt build` completo; y una comprobación de que el DAG de Airflow importa sin errores (DagBag).
> 2. `cd.yml`, que se ejecuta al hacer merge a main: `dbt docs generate` y publicación en GitHub Pages.
> Añade protección de rama para que no se pueda hacer merge si falla el CI.

**Validar:** abre una PR que rompa un test y comprueba que el CI la bloquea; al arreglarla y hacer merge, se publican los docs. Enlaza los docs publicados desde el README.

> Para la entrevista, explica "slim CI" (`dbt build --select state:modified+ --defer --state <manifest de prod>`): cuando hay un entorno de producción, solo se reconstruye lo que cambia y sus dependientes. Aquí no hay producción, así que en el CI se construye todo sobre fixtures, y lo dices tal cual.

## Fase 7 — Nuevo mercado + análisis (~3 h)

**Prompt:**
> Parametriza el proyecto con una variable dbt `market` (bcn, mad…) para añadir Madrid desde Inside Airbnb sin duplicar modelos. Luego haz un notebook de análisis exploratorio sobre `mart_barrio_quarter`: relación entre la densidad de anuncios de alojamiento entero y el alquiler €/m² por barrio, y 3 recomendaciones concretas.

**Validar:** añadir Madrid solo requiere configuración y un seed de barrios, sin tocar los modelos; el notebook responde a la pregunta de negocio.

---

## Total estimado: ~24 h (3 fines de semana, o 2 si recortas la fase 7)

Si vas con prisa por idealista, **las fases 0, 2, 3 y 6 son el mínimo viable**: dan data modeling y CI/CD reales. La 4 y la 5 añaden incremental y Airflow, que es lo siguiente que pesa en la oferta.

## Cómo reflejarlo en el CV (solo cuando esté hecho)

> **BCN Housing Analytics (2026)**: pipeline ELT sobre datos abiertos de alquiler e Inside Airbnb. Modelo dimensional en dbt + ClickHouse (hechos/dimensiones, modelos incrementales, SCD2), orquestado con Airflow y con CI/CD en GitHub Actions (lint, tests y docs publicados).

Ajusta la línea a lo que realmente hayas construido.
