import IGPrices
import logging
from random import seed, random
from abc import abstractmethod, ABC

logger = logging.getLogger(__name__)


class generator(ABC):
    def get_stream(self, item):
        return item


class ig_streamer(generator):
    def __init__(self, api_key, login_details):
        self.api_key = api_key
        self.login_details = login_details

    def trigger_stream(self):
        session = IGPrices.IG_streaming_session(api_key=self.api_key, ulogin_details=self.login_details)











        # if __name__ == '__main__':
        #     # IG Streaming Client
        #     session = IG_streaming_session(api_key=secrets.API_key, ulogin_details=secrets.login_details)
        #
        #     px_subscription = Subscription('MERGE', ['MARKET:CS.D.BITCOIN.CFD.IP', 'MARKET:CS.D.ETHUSD.CFD.IP'],
        #                                    ['UPDATE_TIME', 'BID', 'OFFER'])
        #     px_subscription.addlistener(lambda item: print(item))
        #     session.subscribe(px_subscription)
        #
        #     time.sleep(60)
        #     session.disconnect_session()


def on_prices_update(item_update):
    # print("price: %s " % item_update)
    print(
        "{stock_name:<19}: Time {UPDATE_TIME:<8} - "
        "Bid {BID:>5} - Ask {OFFER:>5}".format(
            stock_name=item_update["name"], **item_update["values"]
        )
    )