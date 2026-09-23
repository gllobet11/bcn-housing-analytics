"""Ingesta EL: descarga Inside Airbnb (Barcelona) y las estadísticas de alquiler
de Barcelona Dades, y las carga sin transformar en la base `raw` de ClickHouse.

Uso:
    python ingestion/load_raw.py [--only airbnb|rent]

Idempotente: recargar un snapshot ya cargado reemplaza sus filas (DROP PARTITION
+ INSERT) sin duplicar.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

import clickhouse_connect
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("load_raw")

ROOT = Path(__file__).resolve().parent.parent
LANDING = ROOT / "data" / "landing"

# ponytail: Inside Airbnb no publica un índice de snapshots archivados por API
# documentada (la lista sale de una static query interna de su Gatsby site).
# Snapshots verificados a mano el 2026-09-23; añade fechas nuevas aquí cuando
# publiquen otro trimestre.
AIRBNB_SNAPSHOTS = ["2026-06-24", "2026-03-21", "2025-12-14", "2025-09-14"]
AIRBNB_DATA_ROOT = "https://data.insideairbnb.com/spain/catalonia/barcelona"

BCN_DADES_BASE = "https://portaldades.ajuntament.barcelona.cat"
BCN_RENT_STATS = {
    "b37xv8wcjh": "rent_price_monthly",  # €/mes
    "5ibudgqbrb": "rent_price_per_m2",  # €/m²
}

CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "raw")


def get_client():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=CLICKHOUSE_DB,
    )


def _create_raw_table(
    client, table: str, columns: list[str], partition_cols: list[str]
) -> None:
    cols_sql = ", ".join(f"`{c}` Nullable(String)" for c in columns)
    # las columnas son Nullable(String); ClickHouse no admite claves de partición nullable
    partition_expr = ", ".join(
        c if c == "_snapshot_date" else f"ifNull(`{c}`, '')" for c in partition_cols
    )
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS `{CLICKHOUSE_DB}`.`{table}` (
            {cols_sql},
            `_snapshot_date` Date,
            `_loaded_at` DateTime
        )
        ENGINE = MergeTree
        PARTITION BY ({partition_expr})
        ORDER BY tuple()
        """
    )
    # esquema fuente puede variar entre snapshots (p.ej. Inside Airbnb añade columnas nuevas)
    existing = {
        row[0]
        for row in client.query(
            f"DESCRIBE TABLE `{CLICKHOUSE_DB}`.`{table}`"
        ).result_rows
    }
    for c in columns:
        if c not in existing:
            client.command(
                f"ALTER TABLE `{CLICKHOUSE_DB}`.`{table}` ADD COLUMN IF NOT EXISTS `{c}` Nullable(String)"
            )


def _replace_partition(
    client,
    table: str,
    snapshot_date: str,
    columns: list[str],
    rows: Iterable[tuple],
    extra_partition: dict[str, str] | None = None,
) -> int:
    """Carga idempotente: tira la partición (snapshot + claves extra, p.ej. stat_id) e inserta de nuevo."""
    extra_partition = extra_partition or {}
    partition_cols = list(extra_partition.keys()) + ["_snapshot_date"]
    partition_values = list(extra_partition.values()) + [snapshot_date]

    _create_raw_table(client, table, columns, partition_cols)
    partition_literal = "(" + ", ".join(f"'{v}'" for v in partition_values) + ")"
    client.command(
        f"ALTER TABLE `{CLICKHOUSE_DB}`.`{table}` DROP PARTITION {partition_literal}"
    )

    snapshot_date_val = date.fromisoformat(snapshot_date)
    loaded_at = datetime.now(timezone.utc)
    all_cols = columns + ["_snapshot_date", "_loaded_at"]
    count = 0
    batch = []
    batch_size = 50_000
    for row in rows:
        batch.append((*row, snapshot_date_val, loaded_at))
        count += 1
        if len(batch) >= batch_size:
            client.insert(table, batch, column_names=all_cols)
            batch = []
    if batch:
        client.insert(table, batch, column_names=all_cols)
    return count


def _download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        log.info("ya en caché: %s", dest)
        return dest
    log.info("descargando %s", url)
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    return dest


def _read_csv_gz(path: Path) -> tuple[list[str], Iterable[list[str]]]:
    f = gzip.open(path, "rt", encoding="utf-8", newline="")
    reader = csv.reader(f)
    header = next(reader)
    return header, reader


def load_airbnb_snapshot(client, snapshot_date: str) -> None:
    landing_dir = LANDING / "airbnb" / snapshot_date

    listings_gz = _download(
        f"{AIRBNB_DATA_ROOT}/{snapshot_date}/data/listings.csv.gz",
        landing_dir / "listings.csv.gz",
    )
    header, rows = _read_csv_gz(listings_gz)
    n = _replace_partition(client, "airbnb_listings", snapshot_date, header, rows)
    log.info("airbnb_listings %s: %d filas", snapshot_date, n)

    calendar_gz = _download(
        f"{AIRBNB_DATA_ROOT}/{snapshot_date}/data/calendar.csv.gz",
        landing_dir / "calendar.csv.gz",
    )
    header, rows = _read_csv_gz(calendar_gz)
    n = _replace_partition(client, "airbnb_calendar", snapshot_date, header, rows)
    log.info("airbnb_calendar %s: %d filas", snapshot_date, n)

    geojson_path = _download(
        f"{AIRBNB_DATA_ROOT}/{snapshot_date}/visualisations/neighbourhoods.geojson",
        landing_dir / "neighbourhoods.geojson",
    )
    geojson = json.loads(geojson_path.read_text(encoding="utf-8"))
    columns = [
        "neighbourhood",
        "neighbourhood_group",
        "geometry_type",
        "geometry_geojson",
    ]
    rows = [
        (
            f["properties"].get("neighbourhood"),
            f["properties"].get("neighbourhood_group"),
            f["geometry"]["type"],
            json.dumps(f["geometry"]),
        )
        for f in geojson["features"]
    ]
    n = _replace_partition(
        client, "airbnb_neighbourhoods", snapshot_date, columns, rows
    )
    log.info("airbnb_neighbourhoods %s: %d filas", snapshot_date, n)


def load_airbnb(client) -> None:
    for snapshot_date in AIRBNB_SNAPSHOTS:
        load_airbnb_snapshot(client, snapshot_date)


def _bcn_dades_client_id() -> str:
    resp = requests.get(
        f"{BCN_DADES_BASE}/portal/config/apimanagerconsumercredentials.json", timeout=30
    )
    resp.raise_for_status()
    return resp.json()["clientId"]


def _validate_rent_contract(payload: dict) -> None:
    """Falla claro si la API no documentada de Barcelona Dades cambia de forma."""
    if "dims" not in payload or "values" not in payload:
        raise ValueError("Contrato roto: la respuesta ya no tiene 'dims'/'values'")
    if not isinstance(payload["dims"], list) or len(payload["dims"]) < 2:
        raise ValueError("Contrato roto: 'dims' ya no tiene [tiempo, territorio]")
    if payload["dims"][0].get("id") != "dim0" or payload["dims"][1].get("id") != "dim1":
        raise ValueError("Contrato roto: los ids de 'dims' ya no son dim0/dim1")
    for v in payload["values"][:5]:
        if not {"dim0", "dim1", "value"} <= v.keys():
            raise ValueError(
                "Contrato roto: una fila de 'values' no tiene dim0/dim1/value"
            )


def _flatten_dim_tree(node: dict, index: dict) -> None:
    """Recorre el árbol de dims[N] y guarda id -> (type, label) para cada hoja/nodo."""
    if "id" in node:
        index[node["id"]] = (node.get("type"), node.get("label"))
    for child in node.get("values", []):
        _flatten_dim_tree(child, index)


def load_rent_stat(client, stat_id: str, snapshot_date: str) -> None:
    client_id = _bcn_dades_client_id()
    resp = requests.get(
        f"{BCN_DADES_BASE}/services/backend/rest/statistic",
        params={"id": stat_id, "language": "ca"},
        headers={"X-IBM-Client-Id": client_id},
        timeout=60,
    )
    resp.raise_for_status()
    payload = resp.json()
    _validate_rent_contract(payload)

    raw_path = LANDING / "bcn_rent" / f"{stat_id}_{snapshot_date}.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    raw_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    time_index: dict = {}
    for node in payload["dims"][0].get("values", []):
        _flatten_dim_tree(node, time_index)
    territory_index: dict = {}
    for node in payload["dims"][1].get("values", []):
        _flatten_dim_tree(node, territory_index)

    columns = [
        "stat_id",
        "period_id",
        "period_type",
        "territory_id",
        "territory_type",
        "territory_label",
        "value",
    ]
    rows = []
    for v in payload["values"]:
        period_type, _ = time_index.get(v["dim0"], (None, None))
        territory_type, territory_label = territory_index.get(v["dim1"], (None, None))
        rows.append(
            (
                stat_id,
                v["dim0"],
                period_type,
                v["dim1"],
                territory_type,
                territory_label,
                str(v["value"]) if v.get("value") is not None else None,
            )
        )

    n = _replace_partition(
        client,
        "bcn_rent",
        snapshot_date,
        columns,
        rows,
        extra_partition={"stat_id": stat_id},
    )
    log.info("bcn_rent[%s] %s: %d filas", stat_id, snapshot_date, n)


def load_rent(client) -> None:
    snapshot_date = datetime.now(timezone.utc).date().isoformat()
    for stat_id in BCN_RENT_STATS:
        load_rent_stat(client, stat_id, snapshot_date)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=["airbnb", "rent"], default=None)
    args = parser.parse_args()

    client = get_client()
    client.command(f"CREATE DATABASE IF NOT EXISTS `{CLICKHOUSE_DB}`")

    if args.only in (None, "airbnb"):
        load_airbnb(client)
    if args.only in (None, "rent"):
        load_rent(client)


if __name__ == "__main__":
    main()
