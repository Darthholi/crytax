# -*- coding: utf-8 -*-

import datetime
import os
from copy import copy

import tqdm

import csv
import logging
import importlib
import click
from currencies import MONEY_FORMATS

logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', datefmt='%H:%M:%S',
                    level=logging.ERROR)


@click.command()
@click.option('--output_file_name', default='export.csv', help='All the downloaded trades go here.')
@click.option('--date_from', default=None, help='Date from')
@click.option('--date_to', default=datetime.date.today().strftime("%Y-%m-%d"), help='Date to')
@click.option('--exch_config', default='exchanges_private.py', help='Date to')
@click.option('--filter_fiats', default=True, help='Filter only conversions to fiats')
def main(output_file_name, date_from, date_to, exch_config, filter_fiats):
    if filter_fiats:
        defformats = copy(MONEY_FORMATS)
        del defformats["BHD"]  # BHD is some bhutan dollar but also BtcHD so lets filter it out for the default
        fiats = list(defformats)
    else:
        fiats = None

    if date_from == "None":
        date_from = None

    if not os.path.exists(exch_config):
        print('Exchanges file {} not exists. '
              'Rename exchanges_example.py to exchanges_private.py and fill credentials.'.format(exch_config))
        return

    date_from = datetime.datetime.strptime(date_from, '%Y-%m-%d') if date_from else None
    date_to = datetime.datetime.strptime(date_to, '%Y-%m-%d')
    print('Date range from {} to {}'.format(date_from, date_to.strftime('%Y-%m-%d')))
    # huobi https://github.com/ccxt/ccxt/issues/6512

    exch_config = (os.path.splitext(exch_config)[0])
    exchangesModule = importlib.import_module(exch_config)
    exchanges = exchangesModule.exchanges

    header = ['exchange', 'datetime', 'symbol', 'cost', 'amount', 'side']
    with open(output_file_name, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(header)

        for exchange in exchanges:
            print()
            print(exchange.name)
            all_my_trades = get_exch_trades(date_from, date_to, exchange, fiats)

            for trade in all_my_trades:
                writer.writerow([exchange.name] +
                                [trade[item] for item in ['datetime', 'symbol', 'cost', 'amount', 'side']])

    print('\nDone, see {}'.format(output_file_name))


def get_exch_trades(date_from, date_to, exchange, filter_currencies):
    filter_currencies = set(filter_currencies)
    if exchange.name.find('Coinbase') >= 0 or exchange.name.find('Binance') >= 0 or exchange.name.find('Huobi') >= 0:
        markets = exchange.load_markets()
        symbols = [symbol for symbol in markets if not set(symbol.split("/")).isdisjoint(filter_currencies)]
    else:
        symbols = [None]

    date_from = int(date_from.timestamp()) * 1000 if date_from else None
    date_to = int(date_to.timestamp() + 24 * 60 * 60) * 1000

    params = {}
    exchange.checkRequiredCredentials()  # raises AuthenticationError
    all_my_trades = []
    for symbol in tqdm.tqdm(symbols):
        while True:
            my_trades = exchange.fetch_my_trades(symbol=symbol, since=date_from,
                                                params=params)
            all_my_trades.extend(my_trades)
            # https://stackoverflow.com/questions/63346907/python-binance-fetchmytrades-gives-only-3-month-of-personal-trades-so-how-do-on
            if exchange.last_response_headers._store.get('cb-after'):
                params['after'] = exchange.last_response_headers._store['cb-after'][1]
            else:
                break
            if len(my_trades) > 0 and my_trades[-1]['timestamp'] >= date_to:
                break
    return [trade for trade in all_my_trades
            if trade['timestamp'] < date_to and
            not set(trade['symbol'].split("/")).isdisjoint(filter_currencies)]


if __name__ == "__main__":
    main()
