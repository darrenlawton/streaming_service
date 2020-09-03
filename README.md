# streaming_service
Summary of what's happening here.
pxserve, creates requests which are stored in db.
each morning, need to check for any requests in the db.
if so, update status to 'in progress' and commence streaming for relevant markets.
Note, to stream, we will need to lock a key for the day. 
Once stream is complete, email file to user.


https://github.com/NerdWalletOSS/kinesis-python/blob/master/src/kinesis/consumer.py

    session = IG_streaming_session(api_key=secrets.API_key, ulogin_details=secrets.login_details)

    px_subscription = Subscription('DISTINCT', ['CHART:CS.D.BITCOIN.CFD.IP:TICK', 'CHART:CS.D.ETHUSD.CFD.IP:TICK'],
                                   ['UTM','BID', 'OFFER', 'LTP', 'LTV'])
    px_subscription.addlistener(lambda item: print(item))
    session.subscribe(px_subscription)

    time.sleep(60)
    session.disconnect_session()


>>> import datetime
>>> s = 1236472051807 / 1000.0
>>> datetime.datetime.fromtimestamp(s).strftime('%Y-%m-%d %H:%M:%S.%f')
'2009-03-08 09:27:31.807000'