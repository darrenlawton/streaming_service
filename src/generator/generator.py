import IGPrices
import logging
from random import seed, random

logger = logging.getLogger(__name__)


def get_rand():
    test = random()
    print("generated %s" %(test))
    return test
