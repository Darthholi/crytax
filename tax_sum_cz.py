# -*- coding: utf-8 -*-

import csv
import datetime
import importlib
import logging
import os
import re
import ast
import defaultdict
from copy import copy
from time import sleep
import pandas as pd

import ccxt.base.errors
import click
import tqdm
from currencies import MONEY_FORMATS
from methodtools import lru_cache

logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', datefmt='%H:%M:%S',
                    level=logging.ERROR)

#@click.group()
#def cli():
#    pass

@click.command()
@click.option('--transact_file_dir', default='./export', help='All the downloaded trades go here.')
@click.option('--date_from', default=None, help='Limiting date from (never go beyond this)')
@click.option('--date_to', default=datetime.date.today().strftime("%Y-%m-%d"), help='Date to')
def main(transact_file_dir, date_from, date_to):
    """
    """
    DATA_FORMAT_HERE = '%Y-%m-%d'
    fname_def = "export"
    fname_end = ".csv"

    if date_from == "None":
        date_from = None

    date_from = datetime.datetime.strptime(date_from, DATA_FORMAT_HERE) if date_from else None
    date_to = datetime.datetime.strptime(date_to, DATA_FORMAT_HERE)

    existing_files = [f for f in os.listdir(transact_file_dir) if os.path.isfile(os.path.join(transact_file_dir, f))]
    match = fname_def + "((19|20)\d{2})-(0[1-9]|1[1,2])-(0[1-9]|[12][0-9]|3[01])" + fname_end
    files_produced_valid = [fn for fn in existing_files if re.match(match, os.path.basename(fn))]
    """
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
    """

    print('Date range from {} to {}'.format(date_from, date_to.strftime(DATA_FORMAT_HERE)))

    #debug first level
    load = os.path.join(transact_file_dir, existing_files[0])

    df = pd.read_csv(load)

    portfolio = defaultdict(lambda: {"amount": 0.0, "avprice": 0.0})  # method of averages!

    for row in df.iterrows():  # debug, maybe faster next time!


        change = row["ChngA"]
        currency = row["CurrA"]

        if change > 0: # nothing to tax, we just update since we buy!
            oldamount = portfolio[currency]["amount"]
            oldprice = portfolio[currency]["avprice"]
            portfolio[currency]["amount"] += change

            pricenow = 0

            portfolio[currency]["avprice"] = (pricenow * change + oldprice * oldamount) / portfolio[currency]["amount"]
        else:
            existing_part = min(portfolio[currency]["amount"], -change)
            if existing_part > 0:







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


if __name__ == '__main__':
    main()
