# -*- coding: utf-8 -*-

import csv
import datetime
import importlib
import logging
import os
import re
import ast
from copy import copy
from time import sleep

import ccxt.base.errors
import click
import tqdm
from currencies import MONEY_FORMATS
from methodtools import lru_cache

logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', datefmt='%H:%M:%S',
                    level=logging.ERROR)

@click.group()
def cli():
    pass

@cli.command()
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


class pricefetchingcache:
    def __init__(self):
        self.exchange = ccxt.binance()

    @lru_cache(maxsize=500)
    def _get_taxprice(self, symbol, minutes_timestamp):
        return self.exchange.fetch_ohlcv(symbol, timeframe='1m', since=minutes_timestamp, limit=1)[0][1]  # val at the beginning, we use it for taxes so it does not matter as far as it is consistent
        """
                Which will return candlestick values for one candle
        [ 
          1516792860000, // timestamp
          11110, // value at beginning of minute, so the value at exactly "2018-01-24 11:20:00"
          11110.29, // highest value between "2018-01-24 11:20:00" and "2018-01-24 11:20:59"
          11050.91, // lowest value between "2018-01-24 11:20:00" and "2018-01-24 11:20:59"
          11052.27, // value just before "2018-01-24 11:21:00"
          39.882601 // The volume traded during this minute
        ]
        """

    def get_taxprice(self, symbol, timestamp, known={}):
        assert isinstance(timestamp, int)
        splits = symbol.split("/")
        if splits[0] == splits[1]:
            return 1

        if symbol in known:
            return known[symbol]

        return self._get_taxprice(symbol, timestamp)


def ccxt_fmt_to_accounting_fmt(exchange_name, item, pricefetching=pricefetchingcache()):
    if item['side'] == "buy":
        firstsign = +1
        secondsign = -1
    else:
        firstsign = -1
        secondsign = +1

    changes = list(item['symbol'].split("/")) + [item['fee']['currency']]  # first comodity that changed, second, third (the fee)
    amounts = [firstsign * item['amount'], secondsign * item['cost'], -item['fee']['cost']]

    # now add prices:
    # curr1/USDT price, curr2/USDT price, curr3/USDT price, EUR/USDT price
    pricesproxy_info = "USDT-EUR"  # proxy-final
    feeproxyinfo = "USDT-EUR"  # proxy-final
    prices = [1,  # first commodity to proxy (pricesinfo)
              1,  # second commodity to proxy (pricesinfo)
              1,  # fee to proxy (usdt)
              1,  # EUR/USDT (final/proxy)
              ]

    if changes[1] == "EUR":  # we do not need proxy, we traded directly with eur
        pricesproxy_info = "EUR-EUR"
        prices[0] = item["price"]
        prices[1] = 1
    else:
        prices[0] = pricefetching.get_taxprice(changes[0] + '/USDT', item['timestamp'], {item['symbol']: item["price"]})
        prices[1] = pricefetching.get_taxprice(changes[1] + '/USDT', item['timestamp'], {item['symbol']: item["price"]})

    # now fee value
    if changes[2] == "EUR":  # fee was paid in euro, no proxy needed
        feeproxyinfo = "EUR-EUR"
        prices[2] = 1
        prices[3] = 1
    else:  # we go through proxy
        prices[2] = pricefetching.get_taxprice(changes[2] + '/USDT', item['timestamp'], {item['symbol']: item["price"]})
        prices[3] = 1.0 / pricefetching.get_taxprice('EUR/USDT', item['timestamp'], {item['symbol']: item["price"]})
        # if that would be usdt/eur we do not need to do 1.0/

    return (exchange_name, item['datetime'],
            changes[0], amounts[0],  # we did this change in this currency of this amount
            changes[1], amounts[1],  # we did this change in this currency of this amount
            changes[2], amounts[2],  # we did this change in this currency of this amount (this was the fee)
            pricesproxy_info,  # FOr taxing, we use proxy-final (or there can be written final-final without proxy)
            prices[0], prices[1],  # these are the prices (open) of the first and second currency
            feeproxyinfo,  # For taxing the fee , we use proxy-final (or there can be written final-final without proxy)
            prices[2], prices[3]  # price for fee/proxy, price proxy/final (usually usdt/eur)
            )


ACCOUNTINGFMT = ("exchange", "datetime",
                 "CurrA", "ChngA",
                 "CurrB", "ChngB",
                 "CurrFee", "ChngFee",
                 "TaxProxy",
                 "PriceA", "PriceB",
                 "FeeProxy",
                 "PriceFee", "proxy/final"
                 )

class PythonLiteralOption(click.Option):

    def type_cast_value(self, ctx, value):
        if value is None:
            return None
        try:
            return ast.literal_eval(value)
        except:
            raise click.BadParameter(value)

@cli.command(name="continuousdl")
@click.option('--output_file_dir', default='./export', help='All the downloaded trades go here.')
@click.option('--min_date_from', default=None, help='Limiting date from (never go beyond this)')
@click.option('--date_to', default=datetime.date.today().strftime("%Y-%m-%d"), help='Date to')
@click.option('--exch_config', default='exchanges_private.py', help='Exchange private settings and keys')
@click.option('--filter_markets', default=None, cls=PythonLiteralOption, help='Only select trades on these markets')
def continuousdl(output_file_dir, min_date_from, date_to, exch_config, filter_markets):
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

    existing_files = [f for f in os.listdir(output_file_dir) if os.path.isfile(os.path.join(output_file_dir, f))]
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
        date_from = max([x for x in [min_date_from, latest_valid_day] if x is not None])
    else:
        date_from = min_date_from

    print('Date range from {} to {}'.format(date_from, date_to.strftime(DATA_FORMAT_HERE)))

    date_last_valid_yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime(DATA_FORMAT_HERE)
    file_temp = os.path.join(output_file_dir, "temp" + date_last_valid_yesterday + ".tmp")
    file_final = os.path.join(output_file_dir, fname_def + date_last_valid_yesterday + fname_end)
    print(f"producing to {file_temp} (yesterdays date as that data will be full, today might not)")

    pricefetching = pricefetchingcache()

    with open(file_temp, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(ACCOUNTINGFMT)

        for exchange in exchanges:
            print()
            print(exchange.name)
            all_my_trades = get_exch_trades(date_from, date_to, exchange, filter_markets=filter_markets)

            for trade in all_my_trades:
                writer.writerow(ccxt_fmt_to_accounting_fmt(exchange.name, trade, pricefetching))

    os.rename(file_temp, file_final)

    print('\nDone, see {}'.format(file_final))

                # TODO: merge vsech techto,
                # Stvoreni dane metodou FIFO
                # todo - vcetne daneni v okamziku smeny za jinou kryptomenu, takze je potreba znat cenu v okamziku obchodu
                # (asi prez btc cenu? nebo jakoukoliv jinou)
                # kdyz se nenajde v ucetni knize, brat jako ze vznikla odjinud a danit bez nakupni ceny.
                # https://stackoverflow.com/questions/70318352/how-to-get-the-price-of-a-crypto-at-a-given-time-in-the-past


def get_exch_trades(date_from, date_to, exchange, add_ref_price="EUR", filter_currencies=None, filter_markets=None):
    if exchange.markets is None:
        exchange.load_markets()
    if filter_markets:
        symbols = filter_markets
        if exchange.markets is not None:
            symbols = [symbol for symbol in symbols if symbol in exchange.markets]
    else:
        filter_currencies = set(filter_currencies) if filter_currencies else None
        if exchange.name.find('Coinbase') >= 0 or exchange.name.find('Binance') >= 0 or exchange.name.find('Huobi') >= 0:
            markets = exchange.load_markets()

            if exchange.name.find('Binance') >= 0:
                markets = [m for m in markets if m.find(":") < 0]

            if filter_currencies:
                symbols = [symbol for symbol in markets if not set(symbol.split("/")).isdisjoint(filter_currencies)]
            else:
                symbols = [symbol for symbol in markets]
        else:
            symbols = [None]

    # exchange.verbose = True

    # date_from_ = int(date_from.timestamp()) * 1000 if date_from else None
    # date_to_ = int(date_to.timestamp() + 24 * 60 * 60) * 1000
    date_from_ = exchange.parse8601(datetime.datetime.combine(date_from,datetime.time()).isoformat()) if date_from else None
    date_to_ = exchange.parse8601(datetime.datetime.combine(date_to,datetime.time()).isoformat())

    params = {}
    exchange.checkRequiredCredentials()  # raises AuthenticationError
    all_my_trades = []
    for symbol in tqdm.tqdm(symbols):
        sleep(0.1)
        while True:
            my_trades = exchange.fetch_my_trades(symbol=symbol, since=date_from_,
                                                 params=params)
            # except ccxt.DDoSProtection:
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

if __name__ == '__main__':
    cli()