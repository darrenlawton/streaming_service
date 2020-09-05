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
from logging.handlers import TimedRotatingFileHandler
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


def get_file_handler():
    if not os.path.exists('logs'):
        os.mkdir('logs')

    file_handler = TimedRotatingFileHandler\
        ('logs/steaming_service.log', when='midnight', utc=True, backupCount=10)
    file_handler.setFormatter \
        (logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
    file_handler.setLevel(logging.WARNING)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(config.LOGGER_NAME)
    logger.setLevel(logging.DEBUG)  # better to have too much log than not enough
    logger.addHandler(get_file_handler())
    logger.propagate = False
    return logger


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Let\'s get streamy.')
    parser.add_argument('-s', help='Number of shards', required=True)
    parser.add_argument('-u', help='AWS profile')
    args = parser.parse_args()

    n_shards = int(args.s)
    aws_profile = args.u
    epic_list = ['CS.D.BITCOIN.CFD.IP', 'CS.D.ETHUSD.CFD.IP']
    stream_name = config.STREAM_NAME
    partition_key = config.PARTITION_KEY

    # create logger
    logger = get_logger(config.LOGGER_NAME)
    logger.info('Streaming service startup')

    # Create kinesis stream
    if aws_profile:
        kinesis_stream = kinesis_stream.kinesisStream(stream_name, n_shards, aws_profile)
    else:
        kinesis_stream = kinesis_stream.kinesisStream(stream_name, n_shards)

    if kinesis_stream.create_stream():
        # Trigger consumer on seperate thread
        cons = multiprocessing.Process(name='consumer', target=trigger_consumer, args=(stream_name, ))
        start_process(cons)

        # create producer
        # make while loop here for input start time and input end time
        producer = kinesis_producer.kinesisProducer(stream_name, partition_key)
        ig_client = generator.ig_streamer(secrets.API_key, secrets.login_details)
        ig_client.trigger_stream(producer.run, epic_list)

        time.sleep(30)
        ig_client.disconnect_session()
        time.sleep(config.CONSUMER_STREAM_FREQ)
        cons.terminate()
        kinesis_stream.terminate_stream()

# python3.6 src/stream_launcher.py -s 1
