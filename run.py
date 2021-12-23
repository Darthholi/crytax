# -*- coding: utf-8 -*-

import datetime
import getopt
import os
import sys

import tqdm

root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(root + '/python')

import csv
import logging
import importlib

logging.basicConfig(#handlers=[logging.FileHandler(filename="log.txt", encoding='utf-8', mode='w')],
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', datefmt='%H:%M:%S',
                    level=logging.ERROR)


def main(argv):
    from forex_python.converter import CurrencyCodes
    from currencies import MONEY_FORMATS
    fiats = MONEY_FORMATS.keys()
    
    outputFileName = 'export.csv'
    # dateFrom = datetime.date.today().replace(day=1).strftime("%Y-%m-%d")
    dateFrom = None
    
    dateTo = datetime.date.today().strftime("%Y-%m-%d")
    exchangesFileName = 'exchanges_private.py'
    
    try:
        opts, args = getopt.getopt(argv, 'he:f:t:o:l',
                                   ['exchangesFileName=', 'dateFrom=', 'dateTo=', 'outputFileName='])
    except getopt.GetoptError as e:
        print(e)
        print('run.py -e <exchangesfile> -f <datefrom> -t <dateto> -o <outputfile> -l(log)')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('run.py -e <exchangesfile> -f <datefrom> -t <dateto> -o <outputfile> -l(log)')
            sys.exit()
        elif opt in ("-e"):
            exchangesFileName = arg
        elif opt in ("-f"):
            dateFrom = arg
            if dateFrom == "None":
                dateFrom = None
        elif opt in ("-t"):
            dateTo = arg
        elif opt in ("-o"):
            outputFileName = arg
    
    if not os.path.exists(exchangesFileName):
        # print('Rename exchanges_example.py to exchanges_private.py and fill credentials!')
        print('Exchanges file {} not exists!'.format(exchangesFileName))
        exit()
    
    dateFrom = datetime.datetime.strptime(dateFrom, '%Y-%m-%d') if dateFrom else None
    dateTo = datetime.datetime.strptime(dateTo, '%Y-%m-%d')
    print('Date range from {} to {}'.format(dateFrom, dateTo.strftime('%Y-%m-%d')))
    dateFrom = int(dateFrom.timestamp()) * 1000 if dateFrom else None
    dateTo = int(dateTo.timestamp() + 24 * 60 * 60) * 1000
    # huobi https://github.com/ccxt/ccxt/issues/6512
    
    exchangesFileName = (os.path.splitext(exchangesFileName)[0])
    exchangesModule = importlib.import_module(exchangesFileName)
    exchanges = exchangesModule.exchanges
    
    # burza, datum, par (fiat-jina mena), zmena fiat, zmena druheho coinu
    header = ['exchange', 'datetime', 'symbol', 'cost', 'amount', 'side']
    with open(outputFileName, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(header)
        
        for exchange in exchanges:
            print()
            print(exchange.name)
            allMyTrades = get_exch_trades(dateFrom, dateTo, exchange, fiats)

            for trade in allMyTrades:
                writer.writerow([exchange.name] +
                                [trade[item] for item in ['datetime', 'symbol', 'cost', 'amount', 'side']])
        
    print('\nDone, see {}'.format(outputFileName))


def get_exch_trades(dateFrom, dateTo, exchange, filter_currencies):
    symbols = []
    filter_currencies = set(filter_currencies)
    if exchange.name.find('Coinbase') >= 0 or exchange.name.find('Binance') >= 0 or exchange.name.find('Huobi') >= 0:
        markets = exchange.load_markets()
        symbols.extend(markets)
        symbols = [symbol for symbol in symbols if not set(symbol.split("/")).isdisjoint(filter_currencies)]
        
    else:
        symbols = [None]
    params = {}
    """
            if exchange.name.find('Huobi') >= 0:
                # dateTo = ccxt.huobi.parse8601(dateTo)  # +2 days allowed range
                param = {
                    'end-date': dateTo  # yyyy-mm-dd format
                }
                markets = exchange.load_markets()
            """
    # print(exchange.requiredCredentials)  # prints required credentials
    exchange.checkRequiredCredentials()  # raises AuthenticationError
    allMyTrades = []
    for symbol in tqdm.tqdm(symbols):
        #if symbol != None:
        #    print(symbol)
        
        while True:
            myTrades = exchange.fetch_my_trades(symbol=symbol, since=dateFrom,
                                                params=params)
            allMyTrades.extend(myTrades)
            # https://stackoverflow.com/questions/63346907/python-binance-fetchmytrades-gives-only-3-month-of-personal-trades-so-how-do-on
            if exchange.last_response_headers._store.get('cb-after'):
                params['after'] = exchange.last_response_headers._store['cb-after'][1]
            else:
                break
            if len(myTrades) > 0 and myTrades[-1]['timestamp'] >= dateTo:
                break
    return [trade for trade in allMyTrades
            if trade['timestamp'] < dateTo and
            not set(trade['symbol'].split("/")).isdisjoint(filter_currencies)]


if __name__ == "__main__":
    main(sys.argv[1:])
