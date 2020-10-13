import requests
from os import getenv
from datetime import datetime
import datetime
from dotenv import load_dotenv, find_dotenv
import boto3
import json


def yesterday(today):
    yesterday = today - datetime.timedelta(days=1)
    yesterday_timestamp = int(yesterday.timestamp()) * 1000
    return yesterday_timestamp


def getToken():
    try:
        TOKEN = getenv("RECENT_SPOTIFY_TOKEN")
        return TOKEN

    except KeyError:
        raise KeyError("RECENT_SPOTIFY_TOKEN is not in system variable")


def getData(requests, url, limit, token, timestamp):
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {}".format(token)
    }

    url = "{}?limit={}&after={}"\
        .format(url, limit, yesterday_timestamp)

    r = requests.get(url, headers=headers)

    return r.json()


def saveJson(s3Object, body):
    s3object.put(
        Body=(body)
    )


if __name__ == "__main__":
    load_dotenv(find_dotenv())
    URL = "https://api.spotify.com/v1/me/player/recently-played"
    TOKEN = getToken()
    S3 = boto3.resource("s3")

    yesterday_timestamp = yesterday(datetime.datetime.now())
    raw_data = getData(requests, URL, 50, TOKEN, yesterday_timestamp)

    if "items" in raw_data.keys():
        filename = "recent_plays_{}.json".format(yesterday_timestamp)
        s3object = S3.Object("spotify-store", filename)
        body = bytes(json.dumps(raw_data).encode("UTF-8"))
        saveJson(s3object, body)

    else:
        raise Exception("Error authenticating")
