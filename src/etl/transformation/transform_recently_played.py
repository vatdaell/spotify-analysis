import boto3
import json
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from os import getenv
from io import StringIO
from datetime import datetime


def getFiles(bucket, prefix="recent_plays_"):
    files = list(bucket.objects.all().filter(Prefix=prefix))
    return files


def loadJsonFile(resource, bucket, filename):
    content_object = resource.Object(bucket, filename)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)


def validateData(dataframe, primary_key):
    return len(dataframe[primary_key].unique()) == len(dataframe)


def uploadData(bucket, buffer):
    bucket.put(
            Body=buffer.getvalue()
        )


def transformTrack(file_content):
    artists = list(map(lambda x: x["track"]["artists"][0]["name"], file_content))
    albums = list(map(lambda x: x["track"]["album"]["name"], file_content))
    tracks = list(map(lambda x: x["track"]["name"], file_content))
    duration = list(map(lambda x: x["track"]["duration_ms"], file_content))
    popularity = list(map(lambda x: x["track"]["popularity"], file_content))
    played_at = list(map(lambda x: x["played_at"], file_content))
    explicit = list(map(lambda x: x["track"]["explicit"], file_content))

    return list(zip(artists, albums, tracks, duration, popularity, played_at, explicit))


COLS = ["artist", "album", "track", "duration",
        "popularity", "played_at", "explicit"]


FOLDER = "weekly_reports"

PREFIX = "raw_json/recent_plays"

if __name__ == "__main__":
    # Load env vars
    load_dotenv(find_dotenv())

    S3 = boto3.resource("s3")
    bucket = S3.Bucket((getenv("S3_BUCKET")))

    files = getFiles(bucket, PREFIX)
    fileNames = list(map(lambda x: x.key, files))
    result = []
    for name in fileNames:
        file_content = loadJsonFile(S3, getenv("S3_BUCKET"), name)
        data_list = transformTrack(file_content["items"])
        result = result + data_list

    result_pd = pd.DataFrame(result, columns=COLS)
    if validateData(result_pd, "played_at"):
        # load to buffer
        csv_buffer = StringIO()
        result_pd.to_csv(csv_buffer)
        filename = "{}/{}.csv".format(FOLDER, datetime.now())
        s3object = S3.Object(getenv("S3_BUCKET"), filename)

        uploadData(s3object, csv_buffer)
