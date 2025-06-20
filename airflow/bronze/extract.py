from pathlib import Path
import requests, os
from bronze.utils import log


def download_json_to_local(s3_url: str, local_path: str) -> str:
    """
        Download a JSON file from a public S3 URL and persist it locally.

        The function performs a simple “extract” step in the ETL pipeline:
        it issues an HTTP GET request to ``s3_url``, ensures the response is
        successful, creates the destination directory if it does not exist,
        writes the file to ``local_path``, and logs progress messages with
        the ``[extract]`` tag so they are easy to trace in Airflow.

        Parameters
        ----------
        s3_url : str
            Fully-qualified HTTP/HTTPS URL pointing to the object in an S3
            bucket (must be publicly accessible).
        local_path : str
            Absolute or relative path on the local filesystem where the file
            should be saved.

        Returns
        -------
        str
            The same ``local_path`` string, making it convenient to chain into
            downstream tasks.

        Raises
        ------
        requests.HTTPError
            If the server returns a 4xx or 5xx status code.
        requests.Timeout
            If the request takes longer than the 30-second timeout.
        requests.RequestException
            For any other network-related issues.

        Example
        -------
        >>> file_path = download_json_to_local(
        ...     "https://qbikas-public.s3.amazonaws.com/raw/orders.json",
        ...     "/tmp/bronze/orders.json"
        ... )
        >>> # `file_path` is now "/tmp/bronze/orders.json"
    """
    #Logs that the download has begun 
    #added the "[extract]" tag refering to the ETL step :D
    log.info(f"[extract] Downloading {s3_url}")
    #request the file from the s3_url 
    response = requests.get(s3_url, timeout=30)
    #if there's no response after 30 seconds, timeout is raised.
    response.raise_for_status()

    #Turns local_path into a Path Object
    #and creates a folder using mkdir
    #parents refer to the middle directories
    #exist_ok -> avoids raising exceptions if the folder already exists
    Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    # opens the file in "w" ->writing mode

    with open(local_path, "w") as fp:
        #writes the body of response.text
        fp.write(response.text)
     #registers that the file was extracted and saved succesfully   
    log.info(f"[extract] File saved – {local_path}")

    #This return is really important, cuz it gives us
    #The location of the data that the method downloaded
    #allowing following pipeline steps to reach it!
    return local_path