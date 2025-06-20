import json, hashlib, logging, os
from uuid import uuid4
from datetime import datetime
from airflow.utils.log.logging_mixin import LoggingMixin

# Extracts LoggingMixin's logger and allows us to 
#Visualize the logs on the airflow UI
log = LoggingMixin().log

#Generates an unique UUID for the pipeline's execution
#and stores it as a String, great to track problems in a specific airflow run :)
RUN_ID = str(uuid4())


def hash_record(record: dict) -> str:
    """
    Compute a deterministic SHA-256 hash for a JSON-serializable record.

    The function canonicalizes the input dictionary with
    ``json.dumps(sort_keys=True)`` so that the same key–value pairs
    always produce identical hashes, regardless of key order.  The
    resulting byte string is then hashed with SHA-256 and returned as
    a hexadecimal string.

    Parameters
    ----------
    record : dict
        Any JSON-serializable Python dictionary.

    Returns
    -------
    str
        A 64-character hexadecimal SHA-256 digest uniquely representing
        the record’s contents.

    Example
    -------
    >>> sample = {"b": 2, "a": 1}
    >>> hash_record(sample)
    '9f0b625aa1f0b4d5f0487e4cef4d9f9d2a1cf5f1c6bd4f9332cbd3d438886b07'
    """
    return hashlib.sha256(
        json.dumps(record, sort_keys=True).encode()
    ).hexdigest()

    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()


def build_metadata(base_path: str, record: dict, valid: bool, error: str | None = None):
    """
    Enrich a raw record with standardized Bronze-layer metadata.

    Each call generates a new dictionary that combines the original
    record (as a JSON string) with traceability and audit fields, such
    as UUID, hash, timestamps, load status, and the pipeline run ID.
    The function is agnostic to whether the record is valid or not; a
    boolean flag and an optional error message capture that state.

    Parameters
    ----------
    base_path : str
        Absolute or relative path of the file from which the record was
        read.  Only the file name is stored in the metadata.
    record : dict
        The raw record in its original structure.
    valid : bool
        ``True`` if the record passed schema validation; ``False``
        otherwise.
    error : str or None, optional
        Validation error message to store when ``valid`` is ``False``.
        Defaults to ``None``.

    Returns
    -------
    dict
        A dictionary containing:
        * **raw** – JSON-encoded original record,
        * **record_validated** – validation boolean,
        * **validation_error** – error message or ``None``,
        * **uuid** – new UUID4 for the row,
        * **record_hash** – deterministic SHA-256 of the record,
        * **ingestion_timestamp** – current UTC timestamp,
        * **source_system** – hard-coded to ``"S3"``,
        * **source_file_name** – file name extracted from *base_path*,
        * **load_status** – ``"loaded"`` if valid else ``"failed"``,
        * **pipeline_run_id** – the global RUN_ID for the pipeline run.

    Example
    -------
    >>> meta = build_metadata(
    ...     "/tmp/bronze/orders.json",
    ...     {"order_id": 123, "amount": 45.6},
    ...     valid=True,
    ... )
    >>> meta["load_status"]
    'loaded'
    """
    return {
        #The raw JSON file
        "raw": json.dumps(record),
        #if valid -> True else -> False
        "record_validated": valid,
        #The error's message or None if everything went ok!
        "validation_error": error,
        #A new Unique id for the registry :)
        "uuid": str(uuid4()),
        #The generated hashcode of the JSON file
        "record_hash": hash_record(record),
        #UTC time stamp
        "ingestion_timestamp": datetime.utcnow(),
        #We know the data is coming from a S3 bucket so 
        #I hardcoded the source haha
        "source_system": "S3",
        #The name of the file
        "source_file_name": os.path.basename(base_path),
        #This allow us to track if the data loaded or it failed!
        "load_status": "loaded" if valid else "failed",
        #The run ID of every single pipeline execution's registry.
        "pipeline_run_id": RUN_ID,
    }