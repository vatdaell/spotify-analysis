import json
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from os import getenv
from io import StringIO
from datetime import datetime
from common.store import Store


def validateData(dataframe, primary_key):
    return pd.Series(dataframe[primary_key]).is_unique


def transformTrack(file_content):
    artists = list(map(lambda x: x["track"]["artists"][0]["name"], file_content))
    albums = list(map(lambda x: x["track"]["album"]["name"], file_content))
    tracks = list(map(lambda x: x["track"]["name"], file_content))
    duration = list(map(lambda x: x["track"]["duration_ms"], file_content))
    popularity = list(map(lambda x: x["track"]["popularity"], file_content))
    played_at = list(map(lambda x: x["played_at"], file_content))
    explicit = list(map(lambda x: x["track"]["explicit"], file_content))
    track_id = list(map(lambda x: x["track"]["id"], file_content))
    return list(zip(artists, albums, tracks, duration, popularity, played_at, explicit, track_id))


COLS = ["artist", "album", "track", "duration",
        "popularity", "played_at", "explicit", "track_id"]
FOLDER = "weekly_reports"
PREFIX = "raw_json/recently_played/recent_plays"


def main():
    # Load env vars
    load_dotenv(find_dotenv())

    store = Store(getenv("S3_BUCKET"))

    files = store.getFiles(PREFIX)
    fileNames = list(map(lambda x: x.key, files))
    result = []
    for name in fileNames:
        file_content = json.loads(store.getFile(name))
        data_list = transformTrack(file_content["items"])
        result = result + data_list

    result_pd = pd.DataFrame(result, columns=COLS).drop_duplicates(subset=["played_at"])

    if validateData(result_pd, "played_at"):
        # load to buffer
        csv_buffer = StringIO()
        result_pd.to_csv(csv_buffer)
        body = csv_buffer.getvalue()

        # save file
        store.saveFile(datetime.now(), FOLDER, body, "", "csv")


if __name__ == "__main__":
    main()
