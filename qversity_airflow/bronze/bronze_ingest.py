from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from datetime import datetime, timedelta
import os, hashlib
# Local imports (inside callable heavy ones)
from . import extract, validate, enrich, load, utils
from qversity_airflow.dags.customer_schema import schema

#Sets the key values that the DAG will use
CONFIG = {
    "s3_url": os.getenv("S3_URL"),
    "local_path": os.getenv("LOCAL_PATH", "/opt/airflow/data/mobile_customers.json"),
    "db_conn": os.getenv("DB_CONN_STR"),
    "schema": "bronze",
    "table": "customers_raw_json",
}

def file_already_loaded(**_):
    """
    Decide whether the pipeline should re-download the source file.

    The function compares the MD5 digest of the local file (configured via
    ``CONFIG["local_path"]``) with the value stored in the Airflow Variable
    ``bronze_last_file_hash``.  It is intended for use with a
    ``ShortCircuitOperator``:

    * **Returns ``True``** when either the file does not yet exist locally
      or its hash differs from the last ingested one → continue the DAG.
    * **Returns ``False``** when the file is identical to the previously
      processed version → short-circuit downstream tasks.

    Parameters
    ----------
    **_ : Any
        Accepts arbitrary keyword arguments from Airflow's task context
        (they are ignored).

    Returns
    -------
    bool
        ``True`` if the file needs to be (re)processed, ``False`` otherwise.

    Example
    -------
    >>> should_run = file_already_loaded()
    >>> # In a ShortCircuitOperator, returning False will skip remaining tasks.
    """

    #Gets the local route where the file was downloaded
    path = CONFIG["local_path"]
    #Checks if the file does not exist yet
    if not os.path.exists(path):
        #True if it doesn't, flagging that the process should continue!
        
        return True  # need to download
    
    #Opens the file in read mode
    with open(path, "rb") as fp:
        #calculates a MD5 hash of the content and turns it into
        #a hex string
        new_hash = hashlib.md5(fp.read()).hexdigest()
    
    #From Airflow Variables gets the hash of the last proccesed file
    #If it's the first file, it'll return none
    last_hash = Variable.get("bronze_last_file_hash", default_var="none")
    #returns true if the hash changed, false if they're identical.
    return new_hash != last_hash




def branch_on_validation(passed: bool, **_):
    """
    Choose the next task based on the validation outcome.

    Designed for use with an Airflow *BranchPythonOperator*.  
    If the validation step passed, the DAG continues with ``load_valid``;  
    otherwise it branches to ``log_invalid_only``.

    Parameters
    ----------
    passed : bool
        Result of the data-validation step (`True` = all expectations met).
    **_ : Any
        Placeholder for Airflow's task context (ignored).

    Returns
    -------
    str
        The ID of the downstream task to execute.

    Example
    -------
    >>> next_task = branch_on_validation(True)
    >>> # -> "load_valid"
    """
    #returns the task_id  that the DAG will jump on
    return "load_valid" if passed else "log_invalid_only"

def set_last_hash(**_):
    """
    Persist the MD5 digest of the latest processed file in an Airflow Variable.

    After a successful load, this helper reads the local file defined in
    ``CONFIG["local_path"]`` and stores its MD5 hex digest under
    ``bronze_last_file_hash``.  Subsequent DAG runs can compare this value to
    decide whether a new file needs to be processed.

    Parameters
    ----------
    **_ : Any
        Placeholder for Airflow's task context (ignored).

    Returns
    -------
    None
        The function's only side effect is updating the Airflow Variable.

    Example
    -------
    >>> set_last_hash()  # Updates Variable with the current file's MD5
    """
    #gets the file location 
    path = CONFIG["local_path"]
    #opens the file in read mode
    with open(path, "rb") as fp:
        #Calculates the MD5 hash of the whole record and saves it
        #as an Airflow Variable 
        Variable.set("bronze_last_file_hash", hashlib.md5(fp.read()).hexdigest())


"""
DAG: bronze_ingest_customers
-----------------------------

Daily ingestion DAG for raw customer data from a public S3 bucket into the Bronze layer.

This DAG is responsible for:
1. Checking whether the daily file has already been processed (via MD5 hash comparison).
2. Downloading the JSON file if needed.
3. Updating the hash registry for future DAG runs.
4. Logging each step via the shared logger.

It includes a TaskGroup (`download_group`) that encapsulates the entire extract phase.

Schedule: Once per day.
Catchup: Disabled.

Tasks
-----
1. `needs_download` (ShortCircuitOperator):  
   - Skips downstream tasks if the local file is identical to the one previously loaded.

2. `download_json` (PythonOperator):  
   - Downloads the raw JSON file from S3 and saves it to disk.

3. `set_last_hash` (PythonOperator):  
   - Calculates the MD5 hash of the file and stores it as a Variable in Airflow.

Dependencies
------------
    needs_download >> download_json >> set_last_hash

Tags
----
bronze, customers

Default Args
------------
- owner: juan_qversity
- retries: 2
- retry_delay: 5 minutes
- SLA: 30 minutes
"""
with DAG(
    dag_id="bronze_ingest_customers",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "juan_qversity",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "sla": timedelta(minutes=30),
    },
    tags=["bronze","customers"],
) as dag:
    # --------------------------------------------------------------
    # TaskGroup: download_group
    #
    # This group handles the "extract" phase of the ETL pipeline.
    #
    # It includes:
    # 1. `needs_download`: A ShortCircuitOperator that checks whether the
    #    file has already been ingested by comparing MD5 hashes.
    #    If the file is identical to the last ingested one, downstream tasks are skipped.
    #
    # 2. `download_json`: Downloads the raw JSON file from a public S3 bucket
    #    and stores it locally using the configuration defined in CONFIG.
    #
    # 3. `set_last_hash`: Updates the stored hash in Airflow Variables
    #    to reflect the latest ingested file.
    #
    # Workflow:
    #     needs_download >> download_json >> set_last_hash
    #
    # Purpose:
    #     Avoid reprocessing the same file and enforce idempotency
    #     during daily ingestion runs.
    # --------------------------------------------------------------
    with TaskGroup("download_group") as download_group:
        need_download=ShortCircuitOperator(
            task_id="needs_download",
            python_callable=file_already_loaded,
        )

        download_file=PythonOperator(
            task_id="download_json",
            python_callable=extract.download_json_to_local,
            op_kwargs={"s3_url": CONFIG["s3_url"],"local_path": CONFIG["local_path"]}
        )

        set_hash=PythonOperator(
            task_id="set_last_hash",
            python_callable=set_last_hash,
            trigger_rule=TriggerRule.All_SUCCESS,
        )

        need_download >> download_file >> set_hash