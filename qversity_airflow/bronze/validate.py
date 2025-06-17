import json, os
from jsonschema import validate, ValidationError
from great_expectations.dataset import PandasDataset
import pandas as pd
from .utils import log, build_metadata

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

def run_great_expectations(df: pd.DataFrame) -> bool:
    """
        Validate the Bronze layer with Great Expectations.

        This function wraps the input DataFrame in a `PandasDataset`,
        defines two basic expectations—namely, that the “uuid” and “raw”
        columns do not contain null values—and then runs the validation.
        The overall result is written to the Airflow logs and returned as a
        boolean so the pipeline can control its execution flow.

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame that already includes the metadata generated during
            the ingestion phase and must contain, at minimum, the “uuid”
            and “raw” columns.

        Returns
        -------
        bool
            `True` if ALL expectations are met; `False` if at least one fails.

        Example
        -------
        >>> success = run_great_expectations(df_bronze)
        >>> if not success:
        ...     raise ValueError("GE validation failed; aborting load to Silver.")
"""
    #Turns the df into a PandaDataset 
    ds: PandasDataset = PandasDataset(df)
    #Expects the "uuid" to have a non null value
    ds.expect_column_values_to_not_be_null("uuid")
    #Expects the data to not be a null value
    ds.expect_column_values_to_not_be_null("raw")
    #Executes all the expectations defined on ds
    #and returns a result
    res = ds.validate()
    #logs if every expecation was met
    log.info(f"[validate] GE success={res.success}")
    #Returns a boolean for the next phases of the pipeline to
    #act accordingly
    return res.success