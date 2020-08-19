from multiprocessing import Pool
import datetime
import requests
import logging
import shutil
import glob
import json
import uuid
import csv
import sys
import os
import re
import config

# load configuration items from config.py
DROPBOX_API = config.DROPBOX_API
DROPBOX_KEY = config.DROPBOX_USER_KEY
ES_BASE = config.ES_BASE
ES_USER = config.ES_USER
ES_PASS = config.ES_PASS
ES_INDX = config.ES_COUNTER_INDX

# configure logging
log = logging.getLogger('counter-info')
log.setLevel(logging.DEBUG)

fmt = logging.Formatter("%(asctime)s [%(name)s][%(levelname)s]: %(message)s")
ch = logging.StreamHandler()
ch.setFormatter(fmt)
ch.setLevel(logging.INFO)

log.addHandler(ch)

# create a dropbox client
log.debug("setting up dropbox client")
dropbox = requests.Session()
if "--proxies" in sys.argv:
    dropbox.proxies = {"http": "http://127.0.0.1:8888",
                       "https": "http://127.0.0.1:8888"}
dropbox.headers.update({"Accept": "application/json",
                        "Content-Type": "application/json",
                        "Authorization": "Bearer " + DROPBOX_KEY})

# create an elasticsearch client
log.debug("setting up elasticsearch client")
elastic = requests.Session()
if "--proxies" in sys.argv:
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


def get_filenames(processed=True, fullpath=False):
    folder_name = "processed" if processed else "unprocessed"
    files_path = "counter-data" + os.sep + folder_name + os.sep + "*.csv"
    log.debug("files path: %s", files_path)
    files_full = glob.glob(files_path)
    log.debug("files full: %s", files_full)
    if fullpath:
        return files_full

    files = [f.split(os.sep)[-1] for f in files_full]
    log.debug("files: %s", files)

    return files


def extract_data(filename):
    lines = []
    with open(filename, "r", encoding="utf-8") as f:
        lines.extend(f.readlines())

    records = []
    for line in lines:
        if re.match(r".+,-?\d,\d\d\.\d\d\.\d\d \d\d\:\d\d", line.strip()):
            fields = line.strip().split(",")
            record = {}
            record["id"] = str(uuid.uuid4())
            record["time"] = datetime.datetime.strptime(fields[2], '%d.%m.%y %H:%M').isoformat() + "+1000"
            record["count"] = int(fields[1])
            record["filename"] = filename.split(os.sep)[-1]
            records.append(record)

    return records


def elastic_save_data(records):
    """function to save a batch of file metadata to elasticsearch"""
    bulk = ""
    for record in records:
        meta = {"update": {"_index": ES_INDX, "_id": record["id"], "_source": True}}
        bulk += json.dumps(meta) + "\n"
        bulk += '{ "doc": ' + json.dumps(record) + ', "doc_as_upsert": true }\n'

    log.debug("saving files: {}".format(len(files)))
    response = elastic.post(ES_BASE + "/_bulk", data=bulk)
    log.debug("response text: {} {}".format(response.status_code, response.reason))

    return len(files)


if __name__ == "__main__":
    log.info("processing starting: {}".format(datetime.datetime.now()))

    # load processed filenames
    processed_files = get_filenames()

    # load unprocessed filenames
    unprocessed_files = get_filenames(False)

    # fetch all counter files metadata from dropbox
    cursor = None
    complete = False
    counter_files = []
    while not complete:
        files, cursor = dropbox_fetch_folder_counter(cursor)
        counter_files.extend(files)
        complete = cursor is None

    # download files
    for counter_file in counter_files:
        if counter_file["name"] in processed_files:
            log.info("file already processed: %s", counter_file["name"])
            continue

        if counter_file["name"] in unprocessed_files:
            log.info("file already downloaded: %s", counter_file["name"])
            continue

        filename = "counter-data/unprocessed/" + counter_file["name"]
        print(filename)
        dropbox_download_file(counter_file["id"], filename)

    # process unprocessed files
    unprocessed_files = get_filenames(False, True)
    for counter_file in unprocessed_files:
        log.info("saving records: %s", counter_file)
        records = extract_data(counter_file)
        elastic_save_data(records)
        shutil.move(counter_file, counter_file.replace("unprocessed", "processed"))

    log.info("processing completed: {}".format(datetime.datetime.now()))
