from multiprocessing import Pool
import datetime
import requests
import logging
import json
import csv
import sys
import config

# load configuration items from config.py
DROPBOX_API = config.DROPBOX_API
DROPBOX_KEY = "j6PNeOYRtnIAAAAAAAAAASMyMZREHY1UGxH3X0dlPr2yMNYI81aJmM3_c_XXjbZx"
DROPBOX_MID = config.DROPBOX_MID
ES_BASE = config.ES_BASE
ES_USER = config.ES_USER
ES_PASS = config.ES_PASS
ES_INDX = config.ES_INDX

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
if "--proxies" in sys.argv:
    dropbox.proxies = {"http": "http://127.0.0.1:8888",
                       "https": "http://127.0.0.1:8888"}
dropbox.headers.update({"Accept": "application/json",
                        "Content-Type": "application/json",
                        "Authorization": "Bearer " + DROPBOX_KEY})


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

    # fetch all the top level folders
    cursor = None
    complete = False
    counter_files = []
    while not complete:
        files, cursor = dropbox_fetch_folder_counter(cursor)
        counter_files.extend(files)
        complete = cursor is None


    for counter_file in counter_files:
        print(counter_file["name"])
