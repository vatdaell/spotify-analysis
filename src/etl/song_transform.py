from common.store import Store
from common.timehelpers import extractDate
from common.apiauth import spotify_authenticate
from dotenv import load_dotenv, find_dotenv
from os import getenv
from io import StringIO
from datetime import datetime
import pandas as pd

FEATURE_PREFIX = "audio_features"
LYRICS_PREFIX = "lyrics"
RECENTLY_PREFIX = "weekly_reports"
FOLDER = "songs"


def validateData(dataframe, primary_key):
    return pd.Series(dataframe[primary_key]).is_unique


def getLatestCSVFile(store, prefix):
    files = store.getFiles(prefix)

    file_names = list(
            filter(
                lambda x: ".csv" in x,
                map(lambda x: x.key, files)
            )
        )

    latest = max(
        list(
            map(lambda x: extractDate(x, prefix, ".csv"),
                file_names)
            )
        )

    filename = "{}/{}.{}".format(prefix, latest, "csv")

    # Get File
    body = store.getFile(filename)
    csv = pd.read_csv(StringIO(body), low_memory=False)

    # Remove weird index col
    return csv.loc[:, csv.columns != "Unnamed: 0"]


if __name__ == "__main__":
    load_dotenv(find_dotenv())

    store = Store(getenv("S3_BUCKET"))

    songs_data = []
    songs_files = store.getFiles(FEATURE_PREFIX)
    songs_fileNames = list(map(lambda x: x.key, songs_files))

    # Load up all the features data
    for name in songs_fileNames:
        body = store.getFile(name)
        csv = pd.read_csv(StringIO(body), low_memory=False)
        songs_data.append(csv)

    csv_features = pd.concat(songs_data)
    csv_features = csv_features.loc[:, csv_features.columns != "Unnamed: 0"]

    csv_lyrics = getLatestCSVFile(store, LYRICS_PREFIX)[["track_id", "lyrics"]]
    csv_lyrics = csv_lyrics.drop_duplicates(subset=["track_id"])

    csv_recently = getLatestCSVFile(store, RECENTLY_PREFIX)[
        [
            "track_id",
            "popularity",
            "explicit"
        ]
    ]
    csv_recently = csv_recently.drop_duplicates(subset=["track_id"])

    joined_data = csv_features.join(
        csv_lyrics.set_index("track_id"),
        on="track_id"
    )

    joined_data = joined_data.join(
        csv_recently.set_index("track_id"),
        on="track_id"
    )
    joined_data = joined_data.drop_duplicates(subset=["track_id"])

    if(validateData(joined_data, "track_id")):
        # load to buffer
        csv_buffer = StringIO()
        joined_data.to_csv(csv_buffer, index=False)
        body = csv_buffer.getvalue()

        # save file
        store.saveFile(datetime.now(), FOLDER, body, "", "csv")
