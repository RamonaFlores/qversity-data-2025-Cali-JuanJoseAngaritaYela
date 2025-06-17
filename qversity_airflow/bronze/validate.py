import json, os
from jsonschema import validate, ValidationError
from great_expectations.dataset import PandasDataset
import pandas as pd
from .utils import log, build_metadata

def split_and_validate(local_path: str, schema: dict):
    """Validate each JSON object against schema & GE expectations."""

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

