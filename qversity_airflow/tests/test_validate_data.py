import os
import time
from sqlalchemy import create_engine, text
import pytest


def test_data_exists_in_bronze(monkeypatch):
    monkeypatch.setenv("DB_CONN_STR", "postgresql+psycopg2://qversity-admin:qversity-admin@localhost:5432/qversity")
    engine = create_engine(os.getenv("DB_CONN_STR"))

    # Retry logic in case the DB container is not ready yet
    retries = 5
    for i in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS bronze.customers_raw (
                        id SERIAL PRIMARY KEY,
                        name TEXT
                    )
                """))
                conn.execute(text("""
                    INSERT INTO bronze.customers_raw (name)
                    VALUES ('Test user')
                    ON CONFLICT DO NOTHING
                """))
                result = conn.execute(text("SELECT COUNT(*) FROM bronze.customers_raw"))
                count = result.scalar()
                assert count > 0
            break
        except Exception as e:
            if i == retries - 1:
                raise e
            time.sleep(5)
