import os
from csv import DictWriter
import datetime
import config
import logging

logger = logging.getLogger(config.LOGGER_NAME)


def write_to_csv(data):
    name = data['name'][len(config.LEFT_ID):(len(data['name'])-len(config.RIGHT_ID))]
    values = data['values']
    filepath = get_file(name, list(values.keys()))

    if values['UTM'] is not None:
        update_time = values['UTM']
        values['UTM'] = convert_timestamp(update_time)
        with open(filepath, 'a+', newline='') as csv_file:
            dict_writer = DictWriter(csv_file, fieldnames=list(values.keys()))
            dict_writer.writerow(values)


def get_file(name, fieldnames):
    filename = name.replace('.', '') + '.csv'
    filepath = os.path.join(config.HOLDING_FOLDER, filename)
    if not os.path.exists(filepath):
        with open(filepath, 'w') as csv_file:
            dict_writer = DictWriter(csv_file, fieldnames=fieldnames)
            dict_writer.writeheader()

    return filepath


def convert_timestamp(update_time):
    s = int(update_time) / 1000.0
    return datetime.datetime.fromtimestamp(s).strftime('%Y-%m-%d %H:%M:%S.%f')
