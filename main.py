from src.kinesis_streaming import kinesis_stream
from src.kinesis_streaming import kinesis_producer
from src.kinesis_streaming import kinesis_consumer
from src.generator import generator
import config
import os, sys
import argparse
import multiprocessing
from datetime import datetime, timedelta
import logging
from logging.handlers import TimedRotatingFileHandler
import secrets
import time
import pause


def trigger_consumer(stream_name):
    # Create and run consumer
    consumer = kinesis_consumer.consumeData(stream_name, config.SHARD_ID, config.ITERATOR_TYPE)
    consumer.run()


def start_process(process_obj):
    if isinstance(process_obj, multiprocessing.context.Process):
        process_obj.start()
        logger.info(process_obj.name + " process started at %s ." % datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def get_file_handler():
    if not os.path.exists('logs'):
        os.mkdir('logs')

    file_handler = TimedRotatingFileHandler\
        ('logs/steaming_service.log', when='midnight', utc=True, backupCount=10)
    file_handler.setFormatter \
        (logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
    file_handler.setLevel(logging.INFO)
    return file_handler


def get_logger(logger_name):
    logger = logging.getLogger(config.LOGGER_NAME)
    logger.setLevel(logging.INFO)  # better to have too much log than not enough
    logger.addHandler(get_file_handler())
    logger.propagate = False
    return logger


def time_keeper(start_date, streaming_time):
    dt = datetime(start_date.year, start_date.month, start_date.day, 17, 32, 0, 0)
    pause.until(dt)
    logger.info("Time to start streaming.")
    return dt + timedelta(seconds=streaming_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Let\'s get streamy.')
    parser.add_argument('-e', help='List of EPIC identifiers, markets to stream',
                        nargs="*", type=str, required=True)
    parser.add_argument('-d', help='Start date for stream',
                        type=lambda s: datetime.strptime(s, '%d/%m/%Y'), required=True)
    parser.add_argument('-t', help='Number of seconds to stream', type=int,
                        default=86400)
    parser.add_argument('-s', help='Number of shards', required=True)
    parser.add_argument('-u', help='AWS profile')
    args = parser.parse_args()

    n_shards = int(args.s)
    aws_profile = args.u
    epic_list = args.e
    start_date = args.d
    streaming_time = args.t
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

    # Check date, ensure on midnight of date to stream
    # if datetime.now() > start_date: sys.exit("You'll need a time machine for this stream")
    end_time = time_keeper(start_date, streaming_time)
    print(end_time)

    if kinesis_stream.create_stream():
        # Trigger consumer on seperate thread
        cons = multiprocessing.Process(name='consumer', target=trigger_consumer, args=(stream_name, ))
        start_process(cons)

        # create producer
        producer = kinesis_producer.kinesisProducer(stream_name, partition_key)
        ig_client = generator.ig_streamer(secrets.API_key, secrets.login_details)
        ig_client.trigger_stream(producer.run, epic_list)

        # stream data until end_time
        while datetime.now() < end_time:
            time.sleep(1)

        ig_client.disconnect_session()
        # allow consumer additional time to finalise data
        time.sleep(config.CONSUMER_STREAM_FREQ)
        cons.terminate()
        kinesis_stream.terminate_stream()

        # move files to s3


# python3 main.py -e CS.D.BITCOIN.CFD.IP CS.D.ETHUSD.CFD.IP -d 5/9/2020 -t 60 -s 1