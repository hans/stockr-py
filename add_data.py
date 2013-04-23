import json
import futures
import threading
import urllib

from dateutil.parser import parse
import pymongo

cl = pymongo.MongoClient('mongodb://localhost')
quotes = cl.stocks.quotes

API = 'http://dev.markitondemand.com/Api/Quote/json?Symbol={}'
PULL = ['AAPL', 'XOM', 'GE', 'CVX', 'JNJ', 'IBM', 'MSFT', 'GOOG', 'PG', 'PFE']
PULL_INTERVAL = 60

def pull_quote(symbol):
    data = json.loads(urllib.urlopen(API.format(symbol)).read())['Data']

    del data['Status']
    del data['Name']
    data['Timestamp'] = parse(data['Timestamp'])

    quotes.save(data)
    return (data['Symbol'], data['LastPrice'])


def go():
    print '-------------------------'

    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        quote_futures = [executor.submit(pull_quote, q) for q in PULL]
        print [f.result() for f in futures.as_completed(quote_futures)]

    threading.Timer(PULL_INTERVAL, go).start()


if __name__ == '__main__':
    go()
