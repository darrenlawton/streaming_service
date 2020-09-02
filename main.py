from src.kinesis_streaming import kinesis_stream
from src.kinesis_streaming import kinesis_producer
from src.kinesis_streaming import kinesis_consumer
from src.generator import generator
import config
import os
import argparse
import multiprocessing
import datetime
import logging
from logging.handlers import RotatingFileHandler
import secrets
import time


def trigger_consumer(stream_name):
    # Create and run consumer
    consumer = kinesis_consumer.consumeData(stream_name, config.SHARD_ID, config.ITERATOR_TYPE)
    consumer.run()


def start_process(process_obj):
    if isinstance(process_obj, multiprocessing.context.Process):
        process_obj.start()
        print(process_obj.name + " process started at %s ." % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Let\'s get streamy.')
    parser.add_argument('-n', help='Stream name', required=True)
    parser.add_argument('-p', help='Partition key', required=True)
    parser.add_argument('-s', help='Number of shards', required=True)
    parser.add_argument('-u', help='AWS profile')
    args = parser.parse_args()

    input_stream_name = args.n
    input_partition_key = args.p
    n_shards = int(args.s)
    aws_profile = args.u

    if not os.path.exists('logs'):
        os.mkdir('logs')
    file_handler = RotatingFileHandler('logs/steaming_service.log', maxBytes=10240, backupCount=10)
    file_handler.setFormatter\
        (logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.info('Streaming service startup')

    # Create kinesis stream
    if aws_profile:
        kinesis_stream = kinesis_stream.kinesisStream(input_stream_name, n_shards, aws_profile)
    else:
        kinesis_stream = kinesis_stream.kinesisStream(input_stream_name, n_shards)

    if kinesis_stream.create_stream():
        print("stream active")
        cons = multiprocessing.Process(name='consumer', target=trigger_consumer, args=(input_stream_name, ))
        start_process(cons)
        print("consumer launched")
        # create producer
        producer = kinesis_producer.kinesisProducer(input_stream_name, input_partition_key)
        print("producer launched")
        ig_client = generator.ig_streamer(secrets.API_key, secrets.login_details)
        print("ig client created")
        ig_client.trigger_stream(producer.run)
        print("let's stream...")
        time.sleep(90)
        kinesis_stream.terminate_stream()

# python3.6 src/stream_launcher.py -n "test" -p  "test" -s 1
