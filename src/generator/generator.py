from IGPrices.lightstreamer import Subscription
from IGPrices.streaming_client import IG_streaming_session
import logging
import time

logger = logging.getLogger(__name__)


class ig_streamer:
    def __init__(self, api_key, login_details):
        self.api_key = api_key
        self.login_details = login_details

    def trigger_stream(self, listening_method):
        session = IG_streaming_session(api_key=self.api_key, ulogin_details=self.login_details)
        px_subscription = Subscription('MERGE', ['MARKET:CS.D.BITCOIN.CFD.IP', 'MARKET:CS.D.ETHUSD.CFD.IP'],
                                       ['UPDATE_TIME', 'BID', 'OFFER'])
        px_subscription.addlistener(listening_method)
        session.subscribe(px_subscription)
