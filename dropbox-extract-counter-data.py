from multiprocessing import Pool
import datetime
import requests
import logging
import glob
import json
import csv
import sys
import os
import config

# load configuration items from config.py
DROPBOX_API = config.DROPBOX_API
DROPBOX_KEY = config.DROPBOX_USER_KEY
ES_BASE = config.ES_BASE
ES_USER = config.ES_USER
ES_PASS = config.ES_PASS
ES_INDX = config.ES_COUNTER_INDX

# configure logging
log = logging.getLogger('dropbox-info')
log.setLevel(logging.DEBUG)

fmt = logging.Formatter("%(asctime)s [%(name)s][%(levelname)s]: %(message)s")
ch = logging.StreamHandler()
ch.setFormatter(fmt)
ch.setLevel(logging.INFO)

log.addHandler(ch)

# create a dropbox client
log.debug("setting up dropbox client")
dropbox = requests.Session()
# if "--proxies" in sys.argv:
dropbox.proxies = {"http": "http://127.0.0.1:8888",
                   "https": "http://127.0.0.1:8888"}
dropbox.headers.update({"Accept": "application/json",
                        "Content-Type": "application/json",
                        "Authorization": "Bearer " + DROPBOX_KEY})

# create an elasticsearch client
log.debug("setting up elasticsearch client")
elastic = requests.Session()
# if "--proxies" in sys.argv:
elastic.proxies = {"http": "http://127.0.0.1:8888", "https": "http://127.0.0.1:8888"}
elastic.auth = (ES_USER, ES_PASS)
elastic.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
elastic.verify = False


def dropbox_download_file(path, filename):
    headers = {"Accept": "application/octet-stream",
               "Content-Type": "application/octet-stream",
               "Dropbox-API-Arg": json.dumps({"path": path})}

    response = dropbox.post("https://content.dropboxapi.com/2/files/download", headers=headers, stream=True)
    if response.status_code == 200:
        log.info("downlading file: %s", filename)
        with open(filename, 'wb') as f:
            for chunk in response:
                f.write(chunk)


def dropbox_fetch_folder_counter(cursor=None):
    """function to fetch the counter folder from Dropbox"""
    data = {
        "path": "/counter files",
        "recursive": False,
        "include_media_info": False,
        "include_deleted": False,
        "include_has_explicit_shared_members": False,
        "include_mounted_folders": True,
        "include_non_downloadable_files": True
    }

    if cursor is None:
        response = dropbox.post(
            DROPBOX_API + "/files/list_folder", data=json.dumps(data))
    else:
        response = dropbox.post(
            DROPBOX_API + "/files/list_folder/continue", data='{"cursor": cursor}')

    log.debug(response.text)

    data = response.json()

    files = []
    for item in data["entries"]:
        files.append(item)

    cursor = None if data["has_more"] == False else data["cursor"]

    return files, cursor


if __name__ == "__main__":
    start_time = datetime.datetime.now()

    log.info("processing starting: {}".format(start_time))

    # load processed filenames
    processed_files_path = "counter-data" + os.sep + "processed" + os.sep + "*.csv"
    log.debug("processed files path: %s", processed_files_path)
    processed_files_full = glob.glob(processed_files_path)
    log.debug("processed files full: %s", processed_files_full)
    processed_files = [f.split(os.sep)[-1] for f in processed_files_full]
    log.debug("processed files: %s", processed_files)

    # load unprocessed filenames
    unprocessed_files_path = "counter-data" + os.sep + "unprocessed" + os.sep + "*.csv"
    log.debug("processed files path: %s", unprocessed_files_path)
    unprocessed_files_full = glob.glob(unprocessed_files_path)
    log.debug("processed files full: %s", unprocessed_files_full)
    unprocessed_files = [f.split(os.sep)[-1] for f in unprocessed_files_full]
    log.debug("processed files: %s", processed_files)

    # fetch all counter files metadata from dropbox
    cursor = None
    complete = False
    counter_files = []
    while not complete:
        files, cursor = dropbox_fetch_folder_counter(cursor)
        counter_files.extend(files)
        complete = cursor is None

    for counter_file in counter_files[:2]:
        if counter_file["name"] in processed_files:
            log.info("file already processed: %s", counter_file["name"])
            continue

        if counter_file["name"] in unprocessed_files:
            log.info("file already downloaded: %s", counter_file["name"])
            continue

        filename = "counter-data/unprocessed/" + counter_file["name"]
        print(filename)
        dropbox_download_file(counter_file["id"], filename)
