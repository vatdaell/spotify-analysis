from common.db import DB
from common.store import Store
from common.timehelpers import extractDate
from dotenv import load_dotenv, find_dotenv
from os import getenv
from io import StringIO
import pandas as pd

PREFIX = "songs"


def main():
    load_dotenv(find_dotenv())
    HOST = getenv("MYSQL_HOST")
    PORT = getenv("MYSQL_PORT")
    USER = getenv("MYSQL_USER")
    PASS = getenv("MYSQL_PASS")
    DATABASE = getenv("MYSQL_DB")

    db = DB(HOST, PORT, USER, PASS, DATABASE)
    store = Store(getenv("S3_BUCKET"))

    files = store.getFiles(PREFIX)
    file_names = list(
        filter(lambda x: ".csv" in x, map(lambda x: x.key, files))
        )

    latest_date = max(
            list(map(lambda x: extractDate(x, PREFIX, ".csv"), file_names))
        )
    latest_file = "{}/{}.{}".format(PREFIX, latest_date, "csv")

    # Get File
    body = store.getFile(latest_file)
    csv = pd.read_csv(StringIO(body), low_memory=False)
    csv = csv[
            [
                "artist", "album", "track", "track_id", "danceability",
                "energy", "key", "loudness", "mode", "speechiness",
                "acousticness", "instrumentalness", "liveness", "valence",
                "tempo", "duration_ms", "lyrics", "popularity", "explicit"
            ]
        ]
    csv = csv.dropna()
    csv_tuple = [tuple(x) for x in csv.to_numpy()]
    # Load data to sql
    db.insertSongs(csv_tuple)


if __name__ == "__main__":
    main()
