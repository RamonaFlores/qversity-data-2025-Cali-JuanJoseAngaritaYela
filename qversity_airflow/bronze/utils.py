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
    """Return deterministic SHA‑256 hash of raw JSON record."""
    return hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()


def build_metadata(base_path: str, record: dict, valid: bool, error: str | None = None):
    """Add standard metadata fields to a raw record (valid or invalid)."""
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