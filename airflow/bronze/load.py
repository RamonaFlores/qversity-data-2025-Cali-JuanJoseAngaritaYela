"""
bronze.load
===========

Incremental load from the Bronze layer into PostgreSQL.

Accepts the DataFrame in any of the following formats:
* pandas.DataFrame
* str (JSON with orient="records")
* list[dict]

Ensures:
* automatic schema creation,
* batch insertion,
* resilient connection (pool_pre_ping=True).

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
        Persists a DataFrame into PostgreSQL (in *append* mode).

        Parameters
        ----------
        df : DataFrame | str | list[dict]
            Data to insert. If given as ``str`` or ``list``, it will be converted to a DataFrame.
        db_conn : str
            SQLAlchemy connection string (e.g., ``postgresql+psycopg2://…``).
        schema : str
            Target schema (created if it does not exist).
        table : str
            Target table.
    """


    # ── 1. Ensure `df` is a DataFrame ─────────────────────────────────────
    if isinstance(df, str):
        df = pd.read_json(df, orient="records")
    elif isinstance(df, list):
        df = pd.DataFrame(df)

    if not isinstance(df, pd.DataFrame):
        raise TypeError("`df` debe ser DataFrame, JSON string o list[dict]")

    # ── 2. Connect and auto-create schema if needed ───────────────────────
    engine = create_engine(db_conn, pool_pre_ping=True)

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

    # ── 3. Transactional batch insertion ──────────────────────────────────
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
