import boto3
from dotenv import load_dotenv, find_dotenv
from os import getenv
import pandas as pd
from io import StringIO
from sqlalchemy import create_engine
import pymysql

PREFIX = "weekly_reports"

def getFiles(bucket, prefix=PREFIX):
    files = list(bucket.objects.all().filter(Prefix=prefix))
    return files


def getCSVFileNames(files):
    return list(filter(lambda x: ".csv" in x, map(lambda x: x.key, files)))


def extractDate(name, prefix=PREFIX, fileType=".csv"):
    prefixLen = len(prefix)
    fileTypeLen = len(fileType)
    return name[prefixLen+1:-fileTypeLen]


def loadCSVfile(resource, bucket, filename):
    content_object = resource.Object(bucket, filename)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return pd.read_csv(StringIO(file_content), low_memory=False)


if __name__ == "__main__":
    load_dotenv(find_dotenv())

    S3 = boto3.resource("s3")
    bucket = S3.Bucket((getenv("S3_BUCKET")))

    files = getFiles(bucket)

    # Get all the csv files
    fileNames = getCSVFileNames(files)
    dates = list(map(lambda x: extractDate(x), fileNames))
    latest_csv_date = max(dates)
    latest_file_name = list(filter(lambda x: latest_csv_date in x, fileNames))

    if len(latest_file_name) != 1:
        raise LookupError("The file can't be found")
    else:
        latest_file_name = latest_file_name[0]

    csv = loadCSVfile(S3, getenv("S3_BUCKET"), latest_file_name)
    csv = csv[['artist', 'album', 'track', 'duration', 'popularity',
       'played_at', 'explicit']]

    # Loading to db
    sqlEngine = create_engine(getenv("MYSQL_URL"), pool_recycle=3600)
    dbConnection = sqlEngine.connect()
    TABLE = getenv("TABLE_NAME")

    try:
        frame = csv.to_sql(TABLE, dbConnection, if_exists='replace')

    except ValueError as vx:
        print(vx)

    except Exception as ex:
        print(ex)

    else:
        print("Table Created")

    finally:
        dbConnection.close()
