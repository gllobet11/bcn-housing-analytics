"""Carga los fixtures de `data/fixtures/` (~200 filas por fuente) en `raw` para CI.

Reutiliza la lógica de `load_raw.py` (misma forma de tabla, misma idempotencia)
pero lee de ficheros locales en vez de descargar de Inside Airbnb / Barcelona
Dades, para que el pipeline de dbt se pueda testear en un PR sin red externa.

Uso:
    python ingestion/load_ci_fixtures.py
"""

from __future__ import annotations

import json
from pathlib import Path

from load_raw import (
    BCN_RENT_STATS,
    CLICKHOUSE_DB,
    _flatten_dim_tree,
    _read_csv_gz,
    _replace_partition,
    _validate_rent_contract,
    get_client,
)

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "data" / "fixtures"
CI_SNAPSHOT = "2026-06-24"


def load_airbnb_fixtures(client) -> None:
    snapshot_dir = FIXTURES / "airbnb" / CI_SNAPSHOT

    header, rows = _read_csv_gz(snapshot_dir / "listings.csv.gz")
    n = _replace_partition(client, "airbnb_listings", CI_SNAPSHOT, header, rows)
    print(f"airbnb_listings {CI_SNAPSHOT}: {n} filas (fixture)")

    header, rows = _read_csv_gz(snapshot_dir / "calendar.csv.gz")
    n = _replace_partition(client, "airbnb_calendar", CI_SNAPSHOT, header, rows)
    print(f"airbnb_calendar {CI_SNAPSHOT}: {n} filas (fixture)")

    geojson = json.loads(
        (snapshot_dir / "neighbourhoods.geojson").read_text(encoding="utf-8")
    )
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
    n = _replace_partition(client, "airbnb_neighbourhoods", CI_SNAPSHOT, columns, rows)
    print(f"airbnb_neighbourhoods {CI_SNAPSHOT}: {n} filas (fixture)")


def load_rent_fixtures(client) -> None:
    for stat_id in BCN_RENT_STATS:
        payload = json.loads(
            (FIXTURES / "bcn_rent" / f"{stat_id}.json").read_text(encoding="utf-8")
        )
        _validate_rent_contract(payload)

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
            territory_type, territory_label = territory_index.get(
                v["dim1"], (None, None)
            )
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
            CI_SNAPSHOT,
            columns,
            rows,
            extra_partition={"stat_id": stat_id},
        )
        print(f"bcn_rent[{stat_id}] {CI_SNAPSHOT}: {n} filas (fixture)")


def main() -> None:
    client = get_client()
    client.command(f"CREATE DATABASE IF NOT EXISTS `{CLICKHOUSE_DB}`")
    load_airbnb_fixtures(client)
    load_rent_fixtures(client)


if __name__ == "__main__":
    main()
