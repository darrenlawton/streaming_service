from IGPrices.lightstreamer import Subscription
from IGPrices.streaming_client import IG_streaming_session
import logging
import config

logger = logging.getLogger(config.LOGGER_NAME)


class ig_streamer:
    def __init__(self, api_key, login_details):
        self.api_key = api_key
        self.login_details = login_details
        self.session = IG_streaming_session(api_key=self.api_key, ulogin_details=self.login_details)

    def trigger_stream(self, listening_method, epic_list):
        formatted_epic_list = [config.LEFT_ID + epic + config.RIGHT_ID for epic in epic_list]
        if self.session is not None:
            px_subscription = Subscription(config.LIGHTSTREAMER_SUBSCRIPTION, formatted_epic_list,
                                           config.DATA_TO_STREAM)
            px_subscription.addlistener(listening_method)
            self.session.subscribe(px_subscription)
        else:
            logger.error("IG session not initiated.")

    def disconnect_session(self):
        self.session.disconnect_session()
        self.session = None
