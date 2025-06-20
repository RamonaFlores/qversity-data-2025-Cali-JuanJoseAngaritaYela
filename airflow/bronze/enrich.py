"""
bronze.enrich
=============

Enriquece registros válidos e inválidos con metadatos Bronze
y devuelve un DataFrame para la fase de carga.
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
    """SHA-256 estable (claves ordenadas) de un dict JSON."""
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()


def _build_row(
    raw_obj: Dict[str, Any],
    validated: bool,
    error_msg: str | None,
    source_file: Path,
) -> Dict[str, Any]:
    """Devuelve un dict con metadatos Bronze para un registro."""
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


# ─────────────────────── callable usado en la DAG ───────────────────────
def enrich_records(
    *,
    ti,                 # inyectado por provide_context=True
    local_path: str,
) -> pd.DataFrame:
    """
    Une registros válidos + inválidos y devuelve un DataFrame
    enriquecido para la capa Bronze.
    """

    # Recupera (valid_records, invalid_records) devuelto por validate_json
    valid_records, invalid_records = ti.xcom_pull(
        key="return_value",
        task_ids="validation_group.validate_json",
    )

    source_path = Path(local_path)
    rows: List[Dict[str, Any]] = []

    # Válidos
    for rec in valid_records:
        rows.append(_build_row(rec, True, None, source_path))

    # Inválidos → tuples (registro, error_msg)
    for rec, err in invalid_records:
        rows.append(_build_row(rec, False, err, source_path))

    df = pd.DataFrame(rows)
    log.info("[enrich] Generated DataFrame with %s rows", len(df))

    # ✅ Devuelve JSON registros → XCom soportado y compacto
    return df.to_json(orient="records")
