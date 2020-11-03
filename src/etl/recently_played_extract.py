from os import getenv
from dotenv import load_dotenv, find_dotenv
from common.timehelpers import yesterday
from common.apiauth import spotify_authenticate
from common.store import Store

FOLDER = "raw_json/recently_played"
PREFIX = "recent_plays_"
FILETYPE = "json"

if __name__ == "__main__":
    # Load env vars
    load_dotenv(find_dotenv())

    # Yesterday's timestamp
    yesterday_timestamp = yesterday()

    # Authenticate
    scope = "user-read-recently-played"
    sp = spotify_authenticate(scope)

    # Get last 50 songs played yesterday
    results = sp.current_user_recently_played(limit=50, after=yesterday_timestamp, before=None)

    store = Store(getenv("S3_BUCKET"))
    body = store.encodeJson(results)
    store.saveFile(yesterday_timestamp, FOLDER, body, PREFIX, FILETYPE)
