import os
import sys
from pathlib import Path
import pytest
from requests.exceptions import HTTPError  # ✅ Necesario para el test de URL inválida

# 👇 Asegura que 'dags' pueda ser importado desde GitHub Actions o local
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from dags.ingest_customer_data_dag import download_json
from dotenv import load_dotenv


def test_download_creates_file(tmp_path, monkeypatch):
    monkeypatch.setenv("S3_URL", "https://jsonplaceholder.typicode.com/users")
    test_file = tmp_path / "data.json"
    monkeypatch.setenv("LOCAL_PATH", str(test_file))

    download_json()

    assert os.path.exists(test_file)
    with open(test_file) as f:
        content = f.read()
        assert "username" in content or "name" in content


def test_download_invalid_url(monkeypatch, tmp_path):
    monkeypatch.setenv("S3_URL", "https://jsonplaceholder.typicode.com/invalid_url")
    monkeypatch.setenv("LOCAL_PATH", str(tmp_path / "data.json"))

    with pytest.raises(HTTPError):
        download_json()


def test_download_missing_url(monkeypatch):
    monkeypatch.delenv("S3_URL", raising=False)
    monkeypatch.setenv("LOCAL_PATH", "/tmp/dummy.json")

    with pytest.raises(TypeError):
        download_json()


def test_download_unwritable_path(monkeypatch):
    monkeypatch.setenv("S3_URL", "https://jsonplaceholder.typicode.com/users")
    monkeypatch.setenv("LOCAL_PATH", "/root/forbidden.json")  # Simula falta de permisos

    with pytest.raises(PermissionError):
        download_json()
