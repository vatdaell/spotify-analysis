from datetime import datetime
import datetime


def yesterday(today=datetime.datetime.now()):
    yesterday = today - datetime.timedelta(days=1)
    yesterday_timestamp = int(yesterday.timestamp()) * 1000
    return yesterday_timestamp


def extractDate(name, prefix, fileType):
    prefixLen = len(prefix)
    fileTypeLen = len(fileType)
    return name[prefixLen+1:-fileTypeLen]
