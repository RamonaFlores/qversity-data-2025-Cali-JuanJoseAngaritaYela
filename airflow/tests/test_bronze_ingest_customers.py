"""pytest end‑to‑end test‑suite for the DAG `bronze_ingest_customers`.

Run with:
    pytest qversity_airflow/tests/test_bronze_ingest_customers.py -vv

The test **does not** touch the network or a real database – everything external is mocked.
"""
import json
import hashlib
from pathlib import Path
from datetime import datetime

import pytest
from airflow.models import DagBag, Variable, TaskInstance
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.models.dagrun import DagRun
from unittest import mock

# ---------------------------------------------------------------------------
# ⬇  Fixtures & mocked data
# ---------------------------------------------------------------------------

@pytest.fixture()
def sample_valid_record():
    """Single record that *passes* the JSON‑Schema validation."""
    return {
        "customer_id": 1,
        "first_name": "John",
        "last_name": "Doe",
        "email": "john@example.com",
        "phone_number": "123",
        "age": 25,
        "country": "US",
        "city": "NY",
        "operator": "T-Mobile",
        "plan_type": "pre_pago",
        "monthly_data_gb": 10,
        "registration_date": "2024-01-01",
        "status": "ACTIVE",
        "device_brand": "Apple",
        "device_model": "iPhone",
        "contracted_services": ["voice"],
        "last_payment_date": "2024-02-01",
        "credit_limit": 1000,
        "data_usage_current_month": 2,
        "latitude": 0,
        "longitude": 0,
        "payment_history": [],
        "credit_score": 700,
    }


@pytest.fixture()
def sample_invalid_record(sample_valid_record):
    """Copy `sample_valid_record` and remove a required field to make it invalid."""
    bad = sample_valid_record.copy()
    bad.pop("customer_id")  # violates the schema
    return bad


@pytest.fixture()
def json_payload(sample_valid_record, sample_invalid_record):
    """A JSON string containing one valid and one invalid record."""
    return json.dumps([sample_valid_record, sample_invalid_record])


@pytest.fixture()
def dag():
    """Load the DAG from the DagBag and assert it is present."""
    dagbag = DagBag()
    dag_obj = dagbag.get_dag("bronze_ingest_customers")
    assert dag_obj is not None, "bronze_ingest_customers DAG not found"
    return dag_obj


# ---------------------------------------------------------------------------
# ⬇  Helper to run a task and return the TaskInstance produced state
# ---------------------------------------------------------------------------

def _run_task(task, execution_date):
    """Create a TaskInstance, run it, and return the TI."""
    ti = TaskInstance(task=task, execution_date=execution_date)
    ti.task = task  # required when TI is created manually
    ti.render_templates()
    ti.run(ignore_ti_state=True)
    return ti


# ---------------------------------------------------------------------------
# ⬇  Global mocks
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def mock_airflow_variables(monkeypatch):
    """Mock Variable.get/set with an in‑memory dictionary."""
    store: dict = {}

    def fake_get(key, default_var=None):
        return store.get(key, default_var)

    def fake_set(key, value):
        store[key] = value

    monkeypatch.setattr(Variable, "get", fake_get)
    monkeypatch.setattr(Variable, "set", fake_set)
    yield


@pytest.fixture()
def mock_requests(monkeypatch, json_payload):
    """Mock `requests.get` so it always returns the predefined JSON payload."""

    class FakeResponse:
        status_code = 200

        def raise_for_status(self):
            return None

        @property
        def text(self):
            return json_payload

    monkeypatch.setattr("requests.get", lambda *_, **__: FakeResponse())
    return FakeResponse


@pytest.fixture()
def tmp_local_file(monkeypatch, tmp_path):
    """Provide a temporary LOCAL_PATH and allow real file IO under that path."""
    local_path = tmp_path / "customers.json"
    monkeypatch.setenv("LOCAL_PATH", str(local_path))
    yield local_path


@pytest.fixture()
def mock_engine(monkeypatch):
    """Mock `sqlalchemy.create_engine` so no real DB connection occurs."""

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execution_options(self, **_):
            return self

        def execute(self, *_):
            return None

        def begin(self):
            return self

        def connect(self):
            return self

    monkeypatch.setattr("sqlalchemy.create_engine", lambda *_: DummyConn())


# ---------------------------------------------------------------------------
# 💥  Test cases
# ---------------------------------------------------------------------------

def test_full_run_downloads_and_loads(dag, mock_requests, tmp_local_file, mock_engine):
    """E2E: file is absent → download → validate → enrich → load."""
    exec_date = timezone.utcnow()
    dagrun: DagRun = dag.create_dagrun(
        run_type="manual",
        execution_date=exec_date,
        state=State.RUNNING,
    )

    # Tasks under test
    t_download = dag.get_task("download_json")
    t_validate = dag.get_task("validate_records")
    t_enrich = dag.get_task("enrich_records")
    t_branch = dag.get_task("branch_on_validation")
    t_load = dag.get_task("load_valid")

    assert _run_task(t_download, exec_date).state == State.SUCCESS
    assert _run_task(t_validate, exec_date).state == State.SUCCESS
    assert _run_task(t_enrich, exec_date).state == State.SUCCESS

    branch_ti = _run_task(t_branch, exec_date)
    assert branch_ti.xcom_pull(task_ids=t_branch.task_id) == "load_valid"
    assert _run_task(t_load, exec_date).state == State.SUCCESS


def test_short_circuit_skips_when_hash_matches(dag, tmp_local_file, json_payload):
    """If MD5 hash matches, the ShortCircuitOperator should skip downstream tasks."""
    # Pre‑create file and store hash in Variable
    tmp_local_file.write_text(json_payload)
    md5 = hashlib.md5(json_payload.encode()).hexdigest()
    Variable.set("bronze_last_file_hash", md5)

    exec_date = timezone.utcnow()
    t_download = dag.get_task("download_json")
    ti = _run_task(t_download, exec_date)

    assert ti.state == State.SKIPPED, "Task should be skipped when hash is unchanged"


def test_branch_on_ge_failure_routes_to_log(
    dag,
    mock_requests,
    tmp_local_file,
    monkeypatch,
    mock_engine,
):
    """When GE returns False (no valid records) the branch should call `log_invalid_only`."""
    # Build payload with ZERO valid records to force GE failure
    bad_only_payload = json.dumps([{"foo": "bar"}])

    class BadResp:
        def raise_for_status(self):
            return None

        @property
        def text(self):
            return bad_only_payload

    monkeypatch.setattr("requests.get", lambda *_, **__: BadResp())

    exec_date = timezone.utcnow()
    t_download = dag.get_task("download_json")
    t_validate = dag.get_task("validate_records")
    t_enrich = dag.get_task("enrich_records")
    t_branch = dag.get_task("branch_on_validation")
    t_log_inv = dag.get_task("log_invalid_only")

    for t in (t_download, t_validate, t_enrich):
        _run_task(t, exec_date)

    branch_ti = _run_task(t_branch, exec_date)
    assert branch_ti.xcom_pull(task_ids=t_branch.task_id) == "log_invalid_only"
    assert _run_task(t_log_inv, exec_date).state == State.SUCCESS
