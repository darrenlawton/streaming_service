import logging
import boto3
import time
import pickle
from botocore.exceptions import ClientError
import config
from src.data_storage import writer

logger = logging.getLogger(config.LOGGER_NAME)


class kinesisConsumer:
    def __init__(self, stream_name, shard_id, iterator):
        super().__init__()
        self.client = boto3.client('kinesis')
        self.stream_name = stream_name
        self.shard_id = shard_id
        self.iterator = iterator
        self.stream_freq = self.set_frequency()

    @staticmethod
    def set_frequency(MillisBehindLatest=0):
        stream_frequency = config.CONSUMER_STREAM_FREQ

        try:
            if MillisBehindLatest > 0:
                stream_frequency = stream_frequency / config.CONSUMER_CATCHUP
        except ClientError as e:
            print("could not set consumer frequency: {}".format(e))
            pass

        return stream_frequency

    def run(self):
        """
        Poll stream for new record and pass to processing method
        """
        response = self.client.get_shard_iterator(StreamName=self.stream_name,
                                                  ShardId=self.shard_id,
                                                  ShardIteratorType=self.iterator)
        iteration = response['ShardIterator']
        while True:
            try:
                response = self.client.get_records(ShardIterator=iteration)
                records = response['Records']

                if records:
                    self.process_records(records)

                iteration = response['NextShardIterator']
                self.stream_freq = self.set_frequency(response['MillisBehindLatest'])
                time.sleep(self.stream_freq)

            except ClientError as e:
                logger.error("Error occurred whilst consuming stream {}".format(e))
                time.sleep(1)


class consumeData(kinesisConsumer):
    @staticmethod
    def process_records(records):
        for r in records:
            data = pickle.loads(r['Data'])
            writer.write_to_csv(data)
