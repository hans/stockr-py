import copy

import arff
import pymongo

OUT_FILE = 'stocks.arff'
RELATION_NAME = 'stock_quotes_%s'

ATTRIBUTES_TO_FETCH = ['LastPrice', 'High', 'Low', 'Open', 'Change',
                       'ChangePercent', 'ChangeYTD', 'ChangePercentYTD',
                       'Volume', 'MarketCap']
ALL_ATTRIBUTES = ATTRIBUTES_TO_FETCH + ['NextHigh', 'NextLow', 'NextVolume']

c = pymongo.MongoClient('mongodb://localhost')
quotes = c.stocks.quotes

symbol_ret = quotes.aggregate([{'$group': {'_id': '$Symbol'}}])['result']
symbols = [x['_id'] for x in symbol_ret]


def make_arff(symbol):
    symbol_quotes = list(quotes.find({'Symbol': symbol}))

    data = [prepare_instance_object(instance, i, symbol_quotes)
            for i, instance in enumerate(symbol_quotes[:-1])]

    return arff.dumps(data, relation=RELATION_NAME % symbol,
                      names=ALL_ATTRIBUTES)

def prepare_instance_object(instance, i, all_instances):
    ret = [instance[k] for k in ATTRIBUTES_TO_FETCH]

    next_instance = all_instances[i + 1]
    ret.append(next_instance['High'])
    ret.append(next_instance['Low'])
    ret.append(next_instance['Volume'])

    return ret


if __name__ == '__main__':
    print make_arff('GOOG')
