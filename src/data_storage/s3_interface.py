import logging
import os
import boto3
from botocore.exceptions import ClientError
import config
import glob
from datetime import date

logger = logging.getLogger(config.LOGGER_NAME)


def upload_file(bucket, file_path, object_name):
    """Upload a file to an S3 bucket

    :param bucket: S3 Bucket
    :param file_name: Local file to upload
    :param epic_id: S3 object name
    :return: True if file was uploaded, else False
    """
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_path, bucket, object_name)
    except ClientError as e:
        logger.error(e)
        return False

    return True


def get_object_name(epic_id, stream_date):
    return "/".join([str(stream_date.year), str(stream_date.month), str(stream_date.day), epic_id])


def migrate_local_directory(directory=config.HOLDING_FOLDER, stream_date=date.today()):
    files = glob.glob(os.path.join(directory, '*'))
    upload = None
    if files:
        for file in files:
            epic_id = os.path.basename(file)
            upload = upload_file(config.S3_BUCKET, file, get_object_name(epic_id, stream_date))
    clear_local_directory(directory)
    return upload


def clear_local_directory(directory=config.HOLDING_FOLDER):
    files = glob.glob(os.path.join(directory, '*'))
    if files:
        for file in files:
            os.remove(file)