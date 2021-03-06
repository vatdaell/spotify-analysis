from common.store import Store
from common.timehelpers import extractDate
from common.apiauth import spotify_authenticate
from dotenv import load_dotenv, find_dotenv
from os import getenv
from io import StringIO
from datetime import datetime
import pandas as pd
import numpy as np

PREFIX = "weekly_reports"
FOLDER = "audio_features"

RAW_PREFIX = "raw_json/features"
RAW_EXTENSION = "json"


def validateData(dataframe, primary_key):
    return pd.Series(dataframe[primary_key]).is_unique


def chunks(l, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(l), n):
        # Create an index range for l of n items:
        yield l[i:i+n]


def main():
    load_dotenv(find_dotenv())

    store = Store(getenv("S3_BUCKET"))

    files = store.getFiles(PREFIX)
    file_names = list(filter(lambda x: ".csv" in x, map(lambda x: x.key, files)))

    latest_date = max(list(map(lambda x: extractDate(x, PREFIX, ".csv"), file_names)))
    latest_file = "{}/{}.{}".format(PREFIX, latest_date, "csv")

    # Get File
    body = store.getFile(latest_file)
    csv = pd.read_csv(StringIO(body), low_memory=False)
    csv = csv[['artist', 'album', 'track', 'track_id']].drop_duplicates()

    list_of_ids = csv.track_id.tolist()
    scope = "user-read-recently-played"
    sp = spotify_authenticate(scope)

    # Make a payload of 100 since api has max 100
    id_chunks = list(chunks(list_of_ids, 100))
    # Get the features
    features = []
    for chunk in id_chunks:
        resp = sp.audio_features(chunk)
        features = features + resp
    
    # Store json as raw format in s3
    body = store.encodeJson(features)
    store.saveFile(datetime.now(), RAW_PREFIX, body, "", RAW_EXTENSION)

    # Make panda dataframe of various features
    features_pd = pd.DataFrame(features)

    features_cols = [
        'danceability', 'energy', 'key',
        'loudness', 'mode', 'speechiness', 'acousticness',
        'instrumentalness', 'liveness', 'valence', 'tempo',
        'duration_ms', 'id'
    ]

    features_csv = features_pd[features_cols]
    features_csv = features_csv.rename(columns={"id": "track_id"})

    merged_pd = csv.join(
        features_csv.set_index("track_id"),
        on="track_id"
    )

    if validateData(merged_pd, "track_id"):
        # load to buffer
        csv_buffer = StringIO()
        merged_pd.to_csv(csv_buffer)
        body = csv_buffer.getvalue()

        # save file
        store.saveFile(datetime.now(), FOLDER, body, "", "csv")


if __name__ == "__main__":
    main()
