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
DROPBOX_KEY = config.DROPBOX_KEY
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


# create an elasticsearch client
log.debug("setting up elasticsearch client")
elastic = requests.Session()
if "--proxies" in sys.argv:
    elastic.proxies = {"http": "http://127.0.0.1:8888", "https": "http://127.0.0.1:8888"}
elastic.auth = (ES_USER, ES_PASS)
elastic.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
elastic.verify = False

# create a dropbox client
log.debug("setting up dropbox client")
dropbox = requests.Session()
if "--proxies" in sys.argv:
    dropbox.proxies = {"http": "http://127.0.0.1:8888", "https": "http://127.0.0.1:8888"}
dropbox.headers.update({"Accept": "application/json",
                        "Content-Type": "application/json",
                        "Authorization": "Bearer " + DROPBOX_KEY})


def dropbox_fetch_team_folders(cursor=None):
    """function to fetch the top level folders from Dropbox"""
    if cursor is None:
        response = dropbox.post(DROPBOX_API + "/team/team_folder/list", data="{}")
    else:
        response = dropbox.post(DROPBOX_API + "/team/team_folder/list/continue", data='{"cursor": cursor}')

    data = response.json()

    team_folders = []
    for item in data["team_folders"]:
        team_folders.append(item)

    cursor = None if data["has_more"] == False else data["cursor"]

    return team_folders, cursor


def dropbox_fetch_files(team_folder, start_time, cursor=None):
    """function to fetch all files in the team_folder provided - recursively"""
    team_folder_payload = '{".tag": "namespace_id", "namespace_id": "' + team_folder["team_folder_id"] + '"}'
    if cursor is None:
        payload = {"path": "", "recursive": True}
        response = dropbox.post(
            DROPBOX_API + "/files/list_folder",
            data=json.dumps(payload),
            headers={
                "Dropbox-API-Select-Admin": DROPBOX_MID,
                "Dropbox-API-Path-Root": team_folder_payload})
    else:
        payload = {"cursor": cursor}
        response = dropbox.post(
            DROPBOX_API + "/files/list_folder/continue",
            data=json.dumps(payload),
            headers={"Dropbox-API-Select-Admin": DROPBOX_MID,
                     "Dropbox-API-Path-Root": team_folder_payload})

    data = response.json()

    files = []
    for item in data["entries"]:
        if item[".tag"] == "file" and "." in item["name"]:
            item["extension"] = item["name"].split(".")[-1]
        item["tag"] = item[".tag"]
        item["path_display"] = team_folder["name"] + item["path_display"]
        item["path_lower"] = team_folder["name"].lower() + item["path_lower"]
        item["base_folder"] = team_folder["name"]
        item["level"] = item["path_lower"].count('/')
        item["parent"] = item["path_lower"][:item["path_lower"].rfind('/')]
        item["state"] = "active"
        item["last_indexed_time"] = start_time.strftime("%Y%m%d%H%M%S")
        del item[".tag"]
        files.append(item)

    cursor = None if data["has_more"] == False else data["cursor"]

    return files, cursor


def elastic_fetch_file_ids():
    """Function to get a list of all file ids on elasticsearch"""
    ids = []
    response = elastic.get(f"{ES_BASE}/{ES_INDX}/_doc/_search?q=*:*&_source=false&size=10000&sort=_doc&scroll=1m")
    data = response.json()
    scroll_id = data["_scroll_id"]
    while len(data["hits"]["hits"]) > 0:
        for hit in data["hits"]["hits"]:
            ids.append(hit["_id"])
        response = elastic.get(f"{ES_BASE}/_search/scroll?scroll=1m&scroll_id={scroll_id}")
        data = response.json()

    return ids


def elastic_save_files(files):
    """function to save a batch of file metadata to elasticsearch"""
    bulk = ""
    for file in files:
        if "id" in file:
            meta = {"update": {"_index": ES_INDX, "_id": file["id"], "_source": True}}
            bulk += json.dumps(meta) + "\n"
            bulk += '{ "doc": ' + json.dumps(file) + ', "doc_as_upsert": true }\n'
        else:
            log.error("file missing 'id': %s", file)

    log.debug("saving files: {}".format(len(files)))
    response = elastic.post(ES_BASE + "/_bulk", data=bulk)
    log.debug("response text: {} {}".format(response.status_code, response.reason))

    return len(files)


def elastic_mark_deleted(file_ids):
    """function to save a batch of file metadata to elasticsearch"""
    data = '{ "doc": { "state": "deleted" }, "doc_as_upsert": true }'
    bulk = ""
    for id in file_ids:
        meta = {"update": {"_index": ES_INDX, "_id": id, "_source": True}}
        bulk += json.dumps(meta) + "\n" + data + "\n"

    log.debug("marking deleted files: {}".format(len(file_ids)))
    response = elastic.post(ES_BASE + "/_bulk", data=bulk)
    log.debug("response text: {} {}".format(response.status_code, response.reason))

    return len(file_ids)


def csv_save_files(file_data, filename="dropbox-data.txt"):
    """function to save all file metadata to to a csv file"""
    with open(filename, "w", newline="", encoding="utf-8") as csvfile:
        out = csv.writer(csvfile, delimiter='\t')
        for item in file_data:
            if item["tag"] == "file":
                out.writerow([item["id"],
                              item["parent_shared_folder_id"],
                              item["base_folder"],
                              item["path_display"],
                              item["path_lower"],
                              item["client_modified"],
                              item["server_modified"],
                              item["rev"],
                              item["size"],
                              item["is_downloadable"],
                              item["content_hash"],
                              item["extension"] if "extension" in item else "",
                              item["tag"]])


def process_team_folder(team_folder, start_time):
    """function to process a team folder"""
    log.info("[{}] processing team folder".format(team_folder["name"]))
    file_data = []
    total_saved = 0
    cursor = None
    complete = False
    while not complete:
        files, cursor = dropbox_fetch_files(team_folder, start_time, cursor)
        file_data += files
        total_saved += elastic_save_files(files)
        log.info("[{}] fetched: {}, saved: {}".format(team_folder["name"], len(file_data), total_saved))
        complete = cursor is None

    return file_data


if __name__ == "__main__":
    start_time = datetime.datetime.now()

    log.info("processing starting: {}".format(start_time))

    # fetch all the top level folders
    cursor = None
    complete = False
    team_folders = []
    while not complete:
        folders, cursor = dropbox_fetch_team_folders(cursor)
        team_folders.extend(folders)
        complete = cursor is None

    # process all the top level folders in parallel.
    # this deep gets all child files and folders
    pool = Pool(processes=8)
    data = pool.starmap(process_team_folder, [(tf, start_time) for tf in team_folders])
    pool.close()

    # combine the results of each of the top level folders
    file_data = [f for files in data for f in files]

    log.info("fetching elasticsearch files")
    current_file_ids = elastic_fetch_file_ids()

    log.info("identifying deleted files")
    deleted_file_ids = set(current_file_ids) - set([f["id"] for f in file_data if "id" in f])
    log.info("deleted files: %s", len(deleted_file_ids))

    log.info("marking files as deleted")
    elastic_mark_deleted(deleted_file_ids)

    # save as csv as well
    log.info("saving csv file")
    csv_save_files(file_data)

    log.info("processing complete: {}".format(datetime.datetime.now()))
