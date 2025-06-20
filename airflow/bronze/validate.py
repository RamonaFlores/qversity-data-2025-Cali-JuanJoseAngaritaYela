import json, os
from jsonschema import validate, ValidationError
import great_expectations as ge 
import pandas as pd
from bronze.utils import log, build_metadata

def split_and_validate(local_path: str, schema: dict):
    """
        Load a local JSON file, split its records into valid and invalid buckets,
        and log a validation summary.

        The function opens the file at ``local_path`` (assumed to contain a JSON
        array of objects), validates each object against the provided JSON Schema,
        and returns two lists:

        * **valid** – dictionaries that meet the schema requirements.
        * **invalid** – tuples ``(record, error_message)`` for every failed record.

        A concise status message is written to the Airflow (or shared) logger so
        observability dashboards can track data quality in the Bronze layer.

        Parameters
        ----------
        local_path : str
            Path to the JSON file on disk. The file must contain a list of
            JSON objects.
        schema : dict
            A JSON Schema definition used to validate each record.

        Returns
        -------
        tuple[list[dict], list[tuple[dict, str]]]
            * ``valid`` – List of schema-compliant records.
            * ``invalid`` – List of *(record, validation_error)* tuples.

        Example
        -------
        >>> good, bad = split_and_validate("/tmp/raw/orders.json", order_schema)
        >>> if bad:
        ...     log.warning("Skipping %d bad records", len(bad))
    """
    #Opens the file in reading mode
    with open(local_path) as fp:
        #Loads all the content from the JSON file as a list of 
        #dictionaries
        raw_data = json.load(fp)
    #Valid stores the registries that pass the validation process

    #Invalid stores the touple (registry, and it's error haha) of those
    #registries that fail the validation
    valid, invalid = [], []
    
    #Goes through every registry in the list
    for registry in raw_data:
        #Validates the registry
        try:
            validate(instance=registry, schema=schema)
            valid.append(registry)
        #If the registry aint valid it throws ValidationError exception
        except ValidationError as err:
            invalid.append((registry, str(err)))
    #Logs how many registries where valid and invalid
    #This is a nice to have, to observe how the data is coming from the bucket
    log.info(f"[validate] valid={len(valid)} invalid={len(invalid)}")
    return valid, invalid

