from src.kinesis_streaming import kinesis_stream
from src.kinesis_streaming import kinesis_producer
from src.kinesis_streaming import kinesis_consumer
from src.generator import generator
import config
import os
import argparse
import multiprocessing, sys
import datetime
import signal
import logging
from logging.handlers import RotatingFileHandler


def trigger_producer(stream_name, partition_key, event):
    # Create and run producer. Need to also define generator function for run method.
    producer = kinesis_producer.kinesisProducer(stream_name, partition_key)
    producer.run(generator.get_rand, event)


def trigger_consumer(stream_name, event):
    # Create and run consumer
    consumer = kinesis_consumer.consumeData(stream_name, config.SHARD_ID, config.ITERATOR_TYPE)
    consumer.run(event)


def start_process(process_obj):
    if isinstance(process_obj, multiprocessing.context.Process):
        process_obj.start()
        print(process_obj.name + " process started at %s ." % datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def check_run_time():
    return datetime.datetime.utcnow().weekday() == 0


def stop(sig, frame):
    print("Shutdown signal received: " & str(sig))
    kinesis_stream.terminate_stream()
    sys.exit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Let\'s get streamy.')
    parser.add_argument('-n', help='Stream name', required=True)
    parser.add_argument('-p', help='Partition key', required=True)
    parser.add_argument('-s', help='Number of shards', required=True)
    parser.add_argument('-u', help='AWS profile')
    parser.add_argument('-b', help='Bypass time check', default=False)
    args = parser.parse_args()

    input_stream_name = args.n
    input_partition_key = args.p
    n_shards = int(args.s)
    aws_profile = args.u
    time_check_bypass = args.b

    if not os.path.exists('logs'):
        os.mkdir('logs')
    file_handler = RotatingFileHandler('logs/steaming_service.log', backupCount=10)
    file_handler.setFormatter\
        (logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
    # file_handler.setLevel(logging.ERROR)
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.info('Streaming service startup')

    # Create kinesis stream
    if check_run_time or time_check_bypass:
        if aws_profile:
            kinesis_stream = kinesis_stream.kinesisStream(input_stream_name, n_shards, aws_profile)
        else:
            kinesis_stream = kinesis_stream.kinesisStream(input_stream_name, n_shards)

        if kinesis_stream.create_stream():
            e = multiprocessing.Event()
            prod = multiprocessing.Process(name='producer', target=trigger_producer,
                                           args=(input_stream_name, input_partition_key, e))
            cons = multiprocessing.Process(name='consumer', target=trigger_consumer, args=(input_stream_name, e))

            start_process(prod)
            start_process(cons)

            signal.signal(signal.SIGTERM, stop)
            signal.signal(signal.SIGHUP, stop)

            prod.join(), cons.join()

            kinesis_stream.terminate_stream()

# python3.6 src/stream_launcher.py -n "test" -p  "test" -s 1
