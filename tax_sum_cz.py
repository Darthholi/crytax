# -*- coding: utf-8 -*-

import datetime
import logging
import os
import re
from collections import defaultdict
import pandas as pd

import click

logging.basicConfig(format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s', datefmt='%H:%M:%S',
                    level=logging.ERROR)

@click.command()
@click.option('--transact_file_dir', default='/media/martin/DATA/Data/crypto_tax_exports/', #default='./export',
 help='All the downloaded trades go here.')
@click.option('--date_from', default=None, help='Limiting date from (never go beyond this)')
@click.option('--date_to', default=datetime.date.today().strftime("%Y-%m-%d"), help='Date to')
def main(transact_file_dir, date_from, date_to):
    """


     # TODO: merge vsech techto,
                # Stvoreni dane metodou FIFO
                # todo - vcetne daneni v okamziku smeny za jinou kryptomenu, takze je potreba znat cenu v okamziku obchodu
                # (asi prez btc cenu? nebo jakoukoliv jinou)
                # kdyz se nenajde v ucetni knize, brat jako ze vznikla odjinud a danit bez nakupni ceny.
                # https://stackoverflow.com/questions/70318352/how-to-get-the-price-of-a-crypto-at-a-given-time-in-the-past
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
    load = os.path.join(transact_file_dir, files_produced_valid[0])

    df = pd.read_csv(load, sep=";")

    portfolio = defaultdict(lambda: {"amount": 0.0, "avprice": 0.0})  # method of averages!
    gainstotax = 0

    for id, row in df.iterrows():  # debug, maybe faster next time!

        change = row["ChngA"]
        currency = row["CurrA"]
        pricenow = row["PriceA"]
        if row["TaxProxy"] == "USDT":  # was in usdt, we want it in eur
            pricenow *= row["final(EUR)/proxy(USDT)"]
        taxdelta, portfolio[currency] = procrow(change, currency, pricenow, portfolio[currency])
        gainstotax += taxdelta

        change = row["ChngB"]
        currency = row["CurrB"]
        pricenow = row["PriceB"]
        if row["TaxProxy"] == "USDT":
            pricenow *= row["final(EUR)/proxy(USDT)"]
        taxdelta, portfolio[currency] = procrow(change, currency, pricenow, portfolio[currency])
        gainstotax += taxdelta


        change = row["ChngFee"]
        currency = row["CurrFee"]
        pricenow = row["PriceFee"]
        if row["FeeProxy"] == "USDT":
            pricenow *= row["final(EUR)/proxy(USDT)"]
        taxdelta, portfolio[currency] = procrow(change, currency, pricenow, portfolio[currency], True)  # todo mark this as not profit, but cost
        gainstotax += taxdelta


    print("final comouted portfolio")
    print(portfolio)
    print("gains to tax")
    print(gainstotax)

        
def procrow(change, currency, pricenow, state, taxascost=False):
    taxdelta = 0
    if change > 0:  # nothing to tax, we just update since we buy!
        oldamount = state["amount"]
        oldprice = state["avprice"]
        state["amount"] += change
        state["avprice"] = (pricenow * change + oldprice * oldamount) / state["amount"]
    else:  # we sell stuff
        existing_part = min(state["amount"], -change)
        if existing_part > 0:  # can we sell something?

            state["amount"] -= existing_part
            taxdelta = existing_part * (pricenow - state['avprice'])
            print(f"Selling {currency} was at {state['avprice']} -> {pricenow}, taxing {taxdelta}")

            if existing_part >= state["amount"] or state["amount"] <= 0:
                # we sold everything we have, lets just reset it
                state["amount"] = 0.0
                state["avprice"] = 0.0

        change = change + existing_part  # negative + positive, we make the number smaller
        # now we sell stuff that seems we do not have bought in the first place
        # it measns we do not have the data for that or we have received it through some random means (gifts)
        # and that needs to be taxed!

        if abs(change) > 0:
            print(f"Was selling {currency} for {-change * pricenow} without a recorded buy price, taxing as a full gain")
            taxdelta = -change * pricenow
            assert state["avprice"] == 0.0

    if taxascost:
        taxdelta = -abs(taxdelta)
    return taxdelta, state


if __name__ == '__main__':
    main()
