# Databricks notebook source
import urllib.request
import os

# Go two levels up to reach the project root
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)

import urllib.request
import shutil
from modules.data_loader.file_downloader import download_file

try:
    # Target URL of the public csv file to download
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    # Define and create the local directory for the downloaded file
    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/lookup"

    # Define the full local path for the downloaded file
    local_path = f"{dir_path}/taxi_zone_lookup.csv"

    # Download the file
    download_file(url, dir_path, local_path)

    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File successfully uploaded")
except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File download failed: {str(e)}")