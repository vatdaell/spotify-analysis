from os import getenv
from datetime import datetime
import datetime
from dotenv import load_dotenv, find_dotenv
import boto3
import botocore.exceptions
import json
import logging
import spotipy
from spotipy.oauth2 import SpotifyOAuth

def yesterday(today):
    yesterday = today - datetime.timedelta(days=1)
    yesterday_timestamp = int(yesterday.timestamp()) * 1000
    return yesterday_timestamp


def saveJson(s3Object, body):
    s3object.put(
        Body=(body)
    )


def spotifyAuthenticate(scope):
    return spotipy.Spotify(auth_manager=SpotifyOAuth(scope=scope))


if __name__ == "__main__":
    # Load env vars
    load_dotenv(find_dotenv())

    logging.info("Starting Extraction")

    S3 = boto3.resource("s3")

    # Yesterday's timestamp
    yesterday_timestamp = yesterday(datetime.datetime.now())

    # Authenticate
    scope = "user-read-recently-played"
    sp = spotifyAuthenticate(scope)
    
    # Get last 50 songs played yesterday
    results = sp.current_user_recently_played(limit=50, after=yesterday_timestamp, before=None)

    # Save raw data to s3 store
    try:
        filename = "recent_plays_{}.json".format(yesterday_timestamp)
        s3object = S3.Object(getenv("S3_BUCKET"), filename)
        body = bytes(json.dumps(results).encode("UTF-8"))
        saveJson(s3object, body)
    except botocore.exceptions.ClientError as error:
        logging.error("Error Saving to s3: {}".format(error.message))
        raise error
    logging.info("Extraction Completed")