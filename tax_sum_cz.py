# -*- coding: utf-8 -*-

"""
Currently taxing in CZ works in the way that all gains from trading
existing-real-world-fiat <-> anything should be taxed.
"""

import pandas as pd
import click

from currencies import MONEY_FORMATS


def switchsymbol(symbol):
    return "/".join(symbol.split("/")[::-1])


def normalize_coin_fiat(ds):
    ds = ds.copy()
    fiats = MONEY_FORMATS.keys()
    to_switch = ds["symbol"].apply(lambda x: x.split("/")[-1] not in fiats)
    switchers = ds[to_switch]
    othercost = switchers["amount"].copy()
    otheramount = switchers["cost"].copy()

    ds["amount"][to_switch] = otheramount
    ds["cost"][to_switch] = othercost
    ds["symbol"][to_switch] = ds["symbol"][to_switch].apply(switchsymbol)
    ds["side"][to_switch] = ds["side"][to_switch].apply(lambda x: "sell" if x == "buy" else "buy")

    # now cost is how much the coin amount did cost me in fiat
    # print(export.group_by(["symbol", "side"]).agg("sum"))
    ds["fiat"] = ds["symbol"].apply(lambda x: x.split("/")[-1])
    return ds

def coinbase_normalize_coin_fiat(ds):

    ds = ds[ds["Transaction Type"].isin({"Buy", "Sell"})]

    return pd.DataFrame({"cost": ds["Total (inclusive of fees)"],
    "fiat": ds["Spot Price Currency"],
    "side": ds["Transaction Type"].apply(lambda x: x.lower())
    }, index=None)


def cryptocom_normalize_coin_fiat(ds):
    fiats = MONEY_FORMATS.keys()

    ds = ds[ds["Currency"] != ds["To Currency"]].copy()

    to_switch = ~ds["To Currency"].isin(fiats)
    fiats_buys = ds[to_switch]  # "Currency" is now always fiat

    buys = pd.DataFrame({"fiat": fiats_buys["Currency"], "cost": fiats_buys["Amount"], "amount": ["buy"] * len(fiats_buys)},
                 index=None)

    fiats_sells = ds[~to_switch]

    sells = pd.DataFrame(
        {"fiat": fiats_sells["To Currency"], "cost": fiats_sells["To Amount"], "amount": ["sell"] * len(fiats_sells)},
        index=None)
    return pd.concat([buys, sells])

@click.command()
@click.option('--fileinput', default='export.csv', help='All the downloaded trades from here.')
@click.option('--fileoutput', default='results.csv', help='Taxing results')
@click.option('--coinbaseinput', default=None, help='All the downloaded trades from noncoinbase.')
@click.option('--cryptofiatinput', default=None, help='All the downloaded trades from crypto.com->fiats.')
def tax(fileinput, fileoutput, coinbaseinput, cryptofiatinput):
    all = []
    if coinbaseinput:
        print("Using coinbase exported input. Make sure the file here"
              " contains only the transactions from the years you want to tax,"
              " as coinbase exports everything by default")
        cb = pd.read_csv(coinbaseinput, sep=",", skiprows=7)
        all.append(coinbase_normalize_coin_fiat(cb))

    if cryptofiatinput:
        print("Using crypto.com exported input. Make sure the file here"
              " contains only the transactions from the years you want to tax,"
              " as crypto.com exports everything by default")
        cc = pd.read_csv(cryptofiatinput, sep=",")
        all.append(cryptocom_normalize_coin_fiat(cc))

    if fileinput:
        ccxts = pd.read_csv(fileinput, sep=";")
        ccxts = normalize_coin_fiat(ccxts)
        all.append(ccxts)

    export = pd.concat(all)
    export.groupby(["fiat", "side"]).agg("sum").reset_index()[["fiat", "side", "cost"]].to_csv(fileoutput)


if __name__ == "__main__":
    tax()
