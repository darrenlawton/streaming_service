import os
import sys
import logging
import boto3
import time
from botocore.exceptions import ClientError
import config

logger = logging.getLogger(config.LOGGER_NAME)


class kinesisStream:
    def __init__(self, stream_name, n_shards):
        self.stream_name = stream_name
        self.n_shards = n_shards

        try:
            self.client = boto3.client('kinesis')
        except:
            logger.error("Error, could not configure AWS profile.")
            sys.exit()

    def create_stream(self):
        try:
            self.client.create_stream(StreamName=self.stream_name, ShardCount=self.n_shards)
        except self.client.exceptions.ResourceInUseException:
            logger.debug('stream {} already exists.'.format(self.stream_name))
            pass
        except ClientError as e:
            logger.error('Unable to create kinesis stream: {}'.format(e))

        return self.validate_stream()

    def terminate_stream(self):
        try:
            self.client.delete_stream(StreamName=self.stream_name)
        except ClientError as e:
            logger.error("Unable to delete kinesis stream: {}".format(e))

    def validate_stream(self):
        status = ""

        while status != config.VALID_STREAM:
            try:
                response = self.client.describe_stream(StreamName=self.stream_name)
                status = response.get('StreamDescription').get('StreamStatus')
                time.sleep(1)
            except ClientError as e:
                logger.error("Error found while describing the stream: %s" % e)
                return False

        # Enable enhanced monitoring, if flag set in data_config
        if config.ENHANCED_MONITORING:
            response = self.client.enable_enhanced_monitoring(StreamName=self.stream_name,
                                                              ShardLevelMetrics=config.SHARD_LVL_METRICS)

        logger.debug('kinesis stream active {} '.format(self.stream_name))

        return True

