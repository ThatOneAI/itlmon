import asyncio
import threading
import boto3
import os
from urllib.parse import urlparse
from botocore.exceptions import NoCredentialsError

from itllib import Itl


def split_s3_url(url):
    # Parse the URL
    parsed_url = urlparse(url)

    # Get the netloc (hostname and port)
    netloc = parsed_url.netloc

    # Remove the port number from the netloc
    hostname = netloc.split(":")[0]
    port = netloc.split(":")[1]

    # Construct the endpoint URL
    endpoint_url = f"{parsed_url.scheme}://{hostname}:{port}"

    # Split the path to retrieve the bucket name and object key
    path_parts = parsed_url.path.lstrip("/").split("/", 1)
    bucket_name = path_parts[0]
    object_key = path_parts[1] if len(path_parts) > 1 else None

    return endpoint_url, bucket_name, object_key


class S3FileDownloader:
    def __init__(self, itl, directory, stream, client, bucket, key_prefix):
        self.directory = os.path.normpath(directory)
        self.client = client
        self.stream = stream
        self.bucket = bucket
        self.key_prefix = key_prefix
        self.looper = None
        self.itl = itl
        self.thread = None

        os.makedirs(self.directory, exist_ok=True)

        self.itl.upstreams([self.stream])

        @self.itl.ondata(self.stream)
        async def ondata(*args, **kwargs):
            self.download(*args, **kwargs)

    def download(self, url, event="put", hash=None, timestamp=None, **kwargs):
        if event != "put":
            return

        endpoint_url, bucket_name, object_key = split_s3_url(url)
        if endpoint_url != self.client._endpoint.host:
            return
        if bucket_name != self.bucket:
            return
        if not object_key.startswith(self.key_prefix):
            return

        # Construct the local path
        bucket_relpath = object_key[len(self.key_prefix) :]
        local_path = os.path.join(self.directory, bucket_relpath)

        # Fetch the object
        response = self.client.download_file(bucket_name, object_key, local_path)
        return response
        # response = self.client.get_object(Bucket=bucket_name, Key=object_key)

        # Get the object's data from the response
        # object_data = response['Body'].read()

        # return object_data
