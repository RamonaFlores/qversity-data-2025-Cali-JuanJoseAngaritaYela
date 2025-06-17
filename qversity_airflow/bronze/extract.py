from pathlib import Path
import requests, os
from .utils import log


def download_json_to_local(s3_url: str, local_path: str) -> str:
    """Download JSON file from qbikas's S3 (public bucket) and store locally."""
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