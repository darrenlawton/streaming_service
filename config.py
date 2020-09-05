# Logging config
LOGGER_NAME = 'streaming'

# AWS Kinesis Stream
STREAM_NAME = 'pxserve'
PARTITION_KEY = 'pxserve'
VALID_STREAM = 'ACTIVE'
SHARD_ID = 'shardId-000000000000'
ITERATOR_TYPE = 'TRIM_HORIZON'
ENHANCED_MONITORING = False
SHARD_LVL_METRICS = ['IteratorAgeMilliseconds']
CONSUMER_STREAM_FREQ = 10
CONSUMER_CATCHUP = 2

# AWS S3
S3_BUCKET = 'pxserve'

# Lightstream configurations
LIGHTSTREAMER_SUBSCRIPTION = 'DISTINCT'
DATA_TO_STREAM = ['UTM','BID', 'OFFER', 'LTP', 'LTV']
LEFT_ID = 'CHART:'
RIGHT_ID = ':TICK'

# Data Storage
HOLDING_FOLDER = 'holding_folder'