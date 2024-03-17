# -*- coding: utf-8 -*-

import datetime
import os
import re
from copy import copy
from time import sleep

import ccxt.base.errors
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
@click.option('--exch_config', default='exchanges_private.py', help='Exchange private settings and keys')
@click.option('--filter_fiats', default=False, help='Filter only conversions to fiats')
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


@click.command(name="continuousdl")
@click.option('--output_file_dir', default='./export', help='All the downloaded trades go here.')
@click.option('--min_date_from', default=None, help='Limiting date from (never go beyond this)')
@click.option('--date_to', default=datetime.date.today().strftime("%Y-%m-%d"), help='Date to')
@click.option('--exch_config', default='exchanges_private.py', help='Exchange private settings and keys')
def continuousdl(output_file_dir, min_date_from, date_to, exch_config):
    """
    Download everything from the last successfull download up to YESTERDAY data
    (and maybe something today also, but do not count on it)

    :param output_file_dir:
    :param date_from:
    :param date_to:
    :param exch_config:
    :return:
    """
    DATA_FORMAT_HERE = '%Y-%m-%d'
    fname_def = "export"
    fname_end = ".csv"

    if min_date_from == "None":
        min_date_from = None

    if not os.path.exists(exch_config):
        print('Exchanges file {} not exists. '
              'Rename exchanges_example.py to exchanges_private.py and fill credentials.'.format(exch_config))
        return

    min_date_from = datetime.datetime.strptime(min_date_from, DATA_FORMAT_HERE) if min_date_from else None
    date_to = datetime.datetime.strptime(date_to, DATA_FORMAT_HERE)
    # huobi https://github.com/ccxt/ccxt/issues/6512

    exch_config = (os.path.splitext(exch_config)[0])
    exchangesModule = importlib.import_module(exch_config)
    exchanges = exchangesModule.exchanges

    if not os.path.exists(output_file_dir):
        os.makedirs(output_file_dir)

    existing_files = [f for f in os.listdir(output_file_dir) if os.path.isfile(f)]
    match = fname_def + "((19|20)\d{2})-(0[1-9]|1[1,2])-(0[1-9]|[12][0-9]|3[01])" + fname_end
    files_produced_valid = [fn for fn in existing_files if re.match(match, os.path.basename(fn))]
    parsed_dates = []
    for fn in files_produced_valid:
        datestr = os.path.basename(fn)[len(fname_def):-len(fname_end)]
        date_parsed = datetime.datetime.strptime(datestr, DATA_FORMAT_HERE).date()
        parsed_dates.append(date_parsed)

    if parsed_dates:
        latest_valid_day = max(parsed_dates)
        print(f"Found latest valid data for {latest_valid_day}")
        date_from = max(min_date_from, latest_valid_day)
    else:
        date_from = min_date_from

    print('Date range from {} to {}'.format(min_date_from, date_to.strftime(DATA_FORMAT_HERE)))

    date_last_valid_yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime(DATA_FORMAT_HERE)
    file_to_produce = os.path.join(output_file_dir, fname_def + date_last_valid_yesterday + fname_end)
    print(f"producing to {file_to_produce}")


    header = ['exchange', 'datetime', 'symbol', 'cost', 'amount', 'side']
    with open(file_to_produce, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(header)

        for exchange in exchanges:
            print()
            print(exchange.name)
            all_my_trades = get_exch_trades(date_from, date_to, exchange)

            for trade in all_my_trades:
                writer.writerow([exchange.name] +
                                [trade[item] for item in ['datetime', 'symbol', 'cost', 'amount', 'side']])

    print('\nDone, see {}'.format(file_to_produce))

# TODO: merge vsech techto,
# Stvoreni dane metodou FIFO
# todo - vcetne daneni v okamziku smeny za jinou kryptomenu, takze je potreba znat cenu v okamziku obchodu
# (asi prez btc cenu? nebo jakoukoliv jinou)
# kdyz se nenajde v ucetni knize, brat jako ze vznikla odjinud a danit bez nakupni ceny.
# https://stackoverflow.com/questions/70318352/how-to-get-the-price-of-a-crypto-at-a-given-time-in-the-past



def get_exch_trades(date_from, date_to, exchange, filter_currencies):
    filter_currencies = set(filter_currencies) if filter_currencies else None
    if exchange.name.find('Coinbase') >= 0 or exchange.name.find('Binance') >= 0 or exchange.name.find('Huobi') >= 0:
        markets = exchange.load_markets()

        if exchange.name.find('Binance') >= 0:
            markets = [m for m in markets if m.find(":")<0]

        if filter_currencies:
            symbols = [symbol for symbol in markets if not set(symbol.split("/")).isdisjoint(filter_currencies)]
        else:
            symbols = [symbol for symbol in markets]
    else:
        symbols = [None]

    #exchange.verbose = True

    #date_from_ = int(date_from.timestamp()) * 1000 if date_from else None
    #date_to_ = int(date_to.timestamp() + 24 * 60 * 60) * 1000
    date_from_ = exchange.parse8601(date_from.isoformat())
    date_to_ = exchange.parse8601(date_to.isoformat())

    params = {}
    exchange.checkRequiredCredentials()  # raises AuthenticationError
    all_my_trades = []
    for symbol in tqdm.tqdm(symbols):
        sleep(0.1)
        while True:
            my_trades = exchange.fetch_my_trades(symbol=symbol, since=date_from_,
                                                params=params)
            #except ccxt.DDoSProtection:
            #    sleep(1)
            #    exchange.checkRequiredCredentials()
            #    continue

            all_my_trades.extend(my_trades)
            # https://stackoverflow.com/questions/63346907/python-binance-fetchmytrades-gives-only-3-month-of-personal-trades-so-how-do-on
            if exchange.last_response_headers._store.get('cb-after'):
                params['after'] = exchange.last_response_headers._store['cb-after'][1]
            else:
                break
            if len(my_trades) > 0 and my_trades[-1]['timestamp'] >= date_to_:
                break
    return [trade for trade in all_my_trades
            if trade['timestamp'] < date_to_ and
            (not filter_currencies or not set(trade['symbol'].split("/")).isdisjoint(filter_currencies))]


if __name__ == "__main__":
    main()
