from multiprocessing import Pool
import datetime
import requests
import logging
import json
import csv
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
elastic.auth = (ES_USER, ES_PASS)
elastic.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
elastic.verify = False

# create a dropbox client
log.debug("setting up dropbox client")
dropbox = requests.Session()
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


def dropbox_fetch_files(team_folder, cursor=None):
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
        del item[".tag"]
        files.append(item)

    cursor = None if data["has_more"] == False else data["cursor"]

    return files, cursor


def elastic_save_files(files):
    """function to save a batch of file metadata to elasticsearch"""
    bulk = ""
    for file in files:
        meta = {"index": {"_index": ES_INDX, "_type": "item", "_id": file["id"]}}
        bulk += json.dumps(meta) + "\n" + json.dumps(file) + "\n"

    log.debug("saving files: {}".format(len(files)))
    response = elastic.post(ES_BASE + "/_bulk", data=bulk)
    log.debug("response text: {} {}".format(response.status_code, response.reason))

    return len(files)


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


def process_team_folder(team_folder):
    """function to process a team folder"""
    log.info("[{}] processing team folder".format(team_folder["name"]))
    file_data = []
    total_saved = 0
    cursor = None
    complete = False
    while not complete:
        files, cursor = dropbox_fetch_files(team_folder, cursor)
        file_data += files
        total_saved += elastic_save_files(files)
        log.info("[{}] fetched: {}, saved: {}".format(team_folder["name"], len(file_data), total_saved))
        complete = cursor is None

    return file_data


if __name__ == "__main__":
    log.info("processing starting: {}".format(datetime.datetime.now()))

    # fetch all the top level folders
    cursor = None
    complete = False
    while not complete:
        team_folders, cursor = dropbox_fetch_team_folders(cursor)
        complete = cursor is None

    # process all the top level folders in parallel.
    # this deep gets all child files and folders
    pool = Pool(processes=8)
    data = pool.map(process_team_folder, team_folders)
    pool.close()

    # combine the results of each of the top level folders
    file_data = [file for files in data for file in files]

    # save as csv as well
    csv_save_files(file_data)

    log.info("processing complete: {}".format(datetime.datetime.now()))
