import os
import sys
from pathlib import Path

# 👇 Esto asegura que 'dags' pueda ser importado desde /opt/airflow
sys.path.insert(0, os.path.abspath("/opt/airflow"))

from dags.ingest_customer_data_dag import download_json
from dotenv import load_dotenv


def test_download_creates_file(tmp_path, monkeypatch):
    monkeypatch.setenv("S3_URL", "https://jsonplaceholder.typicode.com/users")
    test_file = tmp_path / "data.json"
    monkeypatch.setenv("LOCAL_PATH", str(test_file))

    download_json()

    assert os.path.exists(test_file)
    # 🧹 Opcional: puedes verificar contenido también si quieres
    with open(test_file) as f:
        content = f.read()
        assert "username" in content or "name" in content
