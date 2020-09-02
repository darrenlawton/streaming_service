import logging
import boto3
import datetime
import threading
import pickle
from botocore.exceptions import ClientError
import config

logger = logging.getLogger(__name__)


class kinesisProducer(threading.Thread):
    def __init__(self, stream_name, partition_key, stream_freq=config.PRODUCER_STREAM_FREQ):
        super().__init__()
        self.client = boto3.client('kinesis')
        self.stream_name = stream_name
        self.partition_key = partition_key
        self.stream_freq = int(stream_freq)

    def put_record(self, data):
        try:
            self.client.put_record(StreamName=self.stream_name, Data=pickle.dumps(data), PartitionKey=self.partition_key)
        except ClientError as e:
            logger.error("Put failed at %s ." % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def run(self, item_update=None):
        """
        :param generator_function: lambda function i.e. stream_data(lambda: generator_function(#args))
        """
        try:
            if item_update is not None:
                self.put_record(item_update)
        except self.client.exceptions.ResourceNotFoundException:
            logger.error("Stream not found. Terminating processes")
        except ClientError as e:
            logger.error("Error occurred whilst streaming {}".format(e))




