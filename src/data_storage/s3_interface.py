import logging
import boto3
from botocore.exceptions import ClientError
import config

logger = logging.getLogger(config.LOGGER_NAME)


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logger.error(e)
        return False

    return True


# Will need to update
def get_object_name(key, fx_pair, stream_date):
    return "/".join([fx_pair, str(stream_date.year), str(stream_date.month), key])
