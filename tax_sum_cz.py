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
    return ds


@click.command()
@click.option('--fileinput', default='export.csv', help='All the downloaded trades from here.')
@click.option('--fileoutput', default='results.csv', help='Taxing results')
def tax(fileinput, fileoutput):
    export = pd.read_csv(fileinput, sep=";")
    export = normalize_coin_fiat(export)
    # now cost is how much the coin amount did cost me in fiat
    # print(export.group_by(["symbol", "side"]).agg("sum"))
    export["fiat"] = export["symbol"].apply(lambda x: x.split("/")[-1])
    export.groupby(["fiat", "side"]).agg("sum").reset_index()[["fiat", "side", "cost"]].to_csv(fileoutput)


if __name__ == "__main__":
    tax()
