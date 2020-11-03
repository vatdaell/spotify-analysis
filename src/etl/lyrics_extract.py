from os import getenv
from dotenv import load_dotenv, find_dotenv
from common.apiauth import genius_authenticate
from common.store import Store
from common.timehelpers import extractDate
from time import sleep
from datetime import datetime
from io import StringIO
import pandas as pd
import re

PREFIX = "weekly_reports"
FOLDER = "lyrics/raw_data"


def get_artist_song(row):
    # sleep because data is being scraped
    sleep(5)
    artist = row["artist"]
    track = row["track"]
    song = genius.search_song(track, artist)
    result = clean_lyrics(song.lyrics)
    return result


def clean_lyrics(lyrics):
    result = re.sub(r"\[.*\]", "", lyrics)
    result = result.replace("\n", " ").replace("  ", " ")
    return result


if __name__ == "__main__":
    # Load env vars
    load_dotenv(find_dotenv())

    store = Store(getenv("S3_BUCKET"))
    genius = genius_authenticate(getenv("GENIUS_ACCESS_TOKEN"))

    files = store.getFiles(PREFIX)
    file_names = list(filter(lambda x: ".csv" in x, map(lambda x: x.key, files)))

    latest_date = max(list(map(lambda x: extractDate(x, PREFIX, ".csv"), file_names)))
    latest_file = "{}/{}.{}".format(PREFIX, latest_date, "csv")

    # Get File
    body = store.getFile(latest_file)
    csv = pd.read_csv(StringIO(body), low_memory=False)
    csv = csv[["artist", "track", "track_id"]].drop_duplicates()

    # Grab lyrics from genius
    csv["lyrics"] = csv.apply(lambda row: get_artist_song(row), axis=1)

    csv_buffer = StringIO()
    csv.to_csv(csv_buffer)
    body = csv_buffer.getvalue()

    store.saveFile(datetime.now(), FOLDER, body, "", "csv")