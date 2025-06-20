"""
bronze.load
===========

Carga incremental de la capa Bronze a PostgreSQL.

Acepta el DataFrame en cualquiera de las formas:
* pandas.DataFrame
* str  (JSON orient="records")
* list[dict]

Garantiza:
* autocreación de esquema,
* inserción en lote,
* conexión resiliente (pool_pre_ping=True).
"""

from __future__ import annotations

from typing import Union, List, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from bronze.utils import log


# --------------------------------------------------------------------------- #
def load_dataframe(
    *,
    df: Union[pd.DataFrame, str, List[Dict]],
    db_conn: str,
    schema: str,
    table: str,
) -> None:
    """
    Persiste un DataFrame en PostgreSQL (modo *append*).

    Parameters
    ----------
    df : DataFrame | str | list[dict]
        Datos a insertar.  Si es ``str`` o ``list`` se convierte a DataFrame.
    db_conn : str
        Cadena de conexión SQLAlchemy (``postgresql+psycopg2://…``).
    schema : str
        Esquema destino (se crea si no existe).
    table : str
        Tabla destino.
    """

    # ── 1. Asegurar que `df` es un DataFrame ───────────────────────────────
    if isinstance(df, str):
        df = pd.read_json(df, orient="records")
    elif isinstance(df, list):
        df = pd.DataFrame(df)

    if not isinstance(df, pd.DataFrame):
        raise TypeError("`df` debe ser DataFrame, JSON string o list[dict]")

    # ── 2. Conexión y autocreación de esquema ──────────────────────────────
    engine = create_engine(db_conn, pool_pre_ping=True)

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

    # ── 3. Inserción transaccional en lote ─────────────────────────────────
    try:
        with engine.begin() as conn:
            df.to_sql(
                name=table,
                con=conn,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
            )
        log.info("[load] Inserted %s rows into %s.%s", len(df), schema, table)
    except SQLAlchemyError as exc:
        log.error("[load] Database insertion failed: %s", exc)
        raise
