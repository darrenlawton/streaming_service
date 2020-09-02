import logging
import boto3
import time, datetime
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

    def run(self, generator_function, event):
        """
        :param generator_function: lambda function i.e. stream_data(lambda: generator_function(#args))
        """
        while True:
            try:
                data = generator_function()
                if data is not None: self.put_record(data)
                time.sleep(self.stream_freq)
            except self.client.exceptions.ResourceNotFoundException:
                event.set()
                logger.error("Stream not found. Terminating processes")
                break
            except ClientError as e:
                logger.error("Error occurred whilst streaming {}".format(e))
                continue



