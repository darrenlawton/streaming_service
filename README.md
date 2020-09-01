# streaming_service
Summary of what's happening here.
pxserve, creates requests which are stored in db.
each morning, need to check for any requests in the db.
if so, update status to 'in progress' and commence streaming for relevant markets.
Note, to stream, we will need to lock a key for the day. 
Once stream is complete, email file to user.


https://github.com/NerdWalletOSS/kinesis-python/blob/master/src/kinesis/consumer.py