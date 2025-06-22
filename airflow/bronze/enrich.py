"""
bronze.enrich
=============

Enriches both valid and invalid records with Bronze metadata
and returns a DataFrame for the loading phase.
"""

from __future__ import annotations

import json
import hashlib
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from bronze.utils import log


# ─────────────────────────── helpers ────────────────────────────
def _hash_record(record: Dict[str, Any]) -> str:
    """Stable SHA-256 (ordered keys) of a JSON dict."""
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()


def _build_row(
    raw_obj: Dict[str, Any],
    validated: bool,
    error_msg: str | None,
    source_file: Path,
) -> Dict[str, Any]:
    """Returns a dict with Bronze metadata for a single record."""
    return {
        "raw": json.dumps(raw_obj, ensure_ascii=False),
        "record_validated": validated,
        "validation_error": error_msg,
        "uuid": hashlib.md5(
            f"{source_file.name}-{datetime.utcnow().isoformat()}".encode()
        ).hexdigest(),
        "record_hash": _hash_record(raw_obj),
        "ingestion_timestamp": datetime.utcnow().isoformat(),
        "source_system": "S3",
        "source_file_name": source_file.name,
        "load_status": "loaded" if validated else "failed",
    }


# ─────────────────────── callable used on the DAG───────────────────────
def enrich_records(
    *,
    ti,                 # inyected by provide_context=True
    local_path: str,
) -> pd.DataFrame:
    """
        Merges valid + invalid records and returns a DataFrame
        enriched for the Bronze layer.
    """

    # gets (valid_records, invalid_records) devuelto por validate_json
    valid_records, invalid_records = ti.xcom_pull(
        key="return_value",
        task_ids="validation_group.validate_json",
    )

    source_path = Path(local_path)
    rows: List[Dict[str, Any]] = []

    # Valid
    for rec in valid_records:
        rows.append(_build_row(rec, True, None, source_path))

    # Invalid → touples (record, error_msg)
    for rec, err in invalid_records:
        rows.append(_build_row(rec, False, err, source_path))

    df = pd.DataFrame(rows)
    log.info("[enrich] Generated DataFrame with %s rows", len(df))

    #  Returns JSON records
    return df.to_json(orient="records")
