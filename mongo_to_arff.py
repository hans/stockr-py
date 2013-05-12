import copy
import sys

import arff
import pymongo

OUT_FILE = 'stocks.arff'
RELATION_NAME = 'stock_quotes_%s'


c = pymongo.MongoClient('mongodb://localhost')
quotes = c.stocks.quotes

symbol_ret = quotes.aggregate([{'$group': {'_id': '$Symbol'}}])['result']
symbols = [x['_id'] for x in symbol_ret]


ATTRIBUTES_TO_FETCH = ['LastPrice', 'Change', 'Volume', 'MarketCap']

REGRESSION_ATTRIBUTES = ATTRIBUTES_TO_FETCH + ['NextHigh', 'NextLow',
                                               'NextVolume']

CLASSIFICATION_ATTRIBUTES = ATTRIBUTES_TO_FETCH + ['NextBehavior']


def prepare_for_regression(instance, i, all_instances):
    """Describe an example using features which will be conducive to
    building a linear regression algorithm."""

    ret = [instance[k] for k in ATTRIBUTES_TO_FETCH]

    next_instance = all_instances[i + 1]
    for next_key in ['High', 'Low', 'Volume']:
        ret.append(next_instance['next_key'])

    return ret


def prepare_for_classification(instance, i, all_instances, change_threshold=0.1):
    """Describe an example using features which are conducive to
    building a classification algorithm.

    Returned instances will be classified (under the attribute
    `NextBehavior`) as 'drop', 'hold', or 'rise.'

    - A 'drop' class indicates that the example which directly follows
      this one has a price which has dropped more than
      `change_threshold` relative to the current price.
    - A 'hold' class indicates that the share prices for this example
      and the one which directly follows it do not change more than +/-
      `change_threshold`.
    - A 'rise' class indicates that the example which directly follows
      this one has a price which has risen more than `change_threshold`
      relative to the current price."""

    ret = [instance[k] for k in ATTRIBUTES_TO_FETCH]

    next_instance = all_instances[i + 1]
    difference = next_instance['LastPrice'] - instance['LastPrice']

    instance_class = 'hold'
    if difference < -change_threshold:
        instance_class = 'drop'
    elif difference > change_threshold:
        instance_class = 'rise'

    ret.append(instance_class)
    return ret


def make_arff(symbol, preparer, attributes):
    symbol_quotes = list(quotes.find({'Symbol': symbol}))

    data = [preparer(instance, i, symbol_quotes)
            for i, instance in enumerate(symbol_quotes[:-1])]

    return arff.dumps(data, relation=RELATION_NAME % symbol,
                      names=attributes)


if __name__ == '__main__':
    symbol = sys.argv[1] if sys.argv[1] else 'GOOG'

    print make_arff(symbol, prepare_for_classification,
                    CLASSIFICATION_ATTRIBUTES)
