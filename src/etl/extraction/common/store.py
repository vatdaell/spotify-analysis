import boto3
import json


class Store(object):
    """
    This class is used to abstract a s3 storage object

    Attributes:
        name (str): The name of the s3 bucket
        store (boto3.resources.factory.s3.ServiceResource): S3 Resource
    """
    def __init__(self, name):
        self.name = name
        self.store = boto3.resource("s3")

    def encodeJson(self, body, encoding="UTF-8"):
        """
        The method to load json into a byte object

        Parameters:
            body (dict): A dict representation of json
            encoding (str): Encoding format, defaults to UTF-8

        Returns:
            bytes: A json object loaded into memory
        """
        return bytes(json.dumps(body).encode(encoding))

    def saveFile(self, filename, folder, body, prefix, fileType):
        """
        The method to load json into a byte object

        Parameters:
            filename (str): Name of filename
            folder (str): Folder name in bucket
            body (byte): Payload of file
            prefix (str): Prefix to attach to file
            fileType (str): file extension

        Returns:
            None
        """
        filename = self._generateFilename(filename, folder, prefix, fileType)
        storeObj = self.store.Object(self.name, filename)
        storeObj.put(Body=(body))

    def _generateFilename(self, name, folder, prefix="recent_plays_", fileType="json"):
        filename = "{}/{}{}.{}".format(folder, prefix, name, fileType)
        return filename
