from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv("/opt/airflow/.env")

def test_data_exists_in_bronze(monkeypatch):
    monkeypatch.setenv("DB_CONN_STR", "postgresql+psycopg2://qversity-admin:qversity-admin@postgres/qversity")
    engine = create_engine(os.getenv("DB_CONN_STR"))

    with engine.connect() as conn:
        # Crear la tabla si no existe (solo para el test)
        conn.execute("""
            CREATE SCHEMA IF NOT EXISTS bronze;
            CREATE TABLE IF NOT EXISTS bronze.customers_raw (
                id SERIAL PRIMARY KEY,
                name TEXT
            );
            INSERT INTO bronze.customers_raw (name) VALUES ('Test user') ON CONFLICT DO NOTHING;
        """)

        result = conn.execute("SELECT COUNT(*) FROM bronze.customers_raw;")
        count = result.fetchone()[0]
        assert count > 0
