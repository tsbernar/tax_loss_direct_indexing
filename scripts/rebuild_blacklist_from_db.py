import datetime
import json
import sqlite3
import sys
from typing import Dict

import click
import munch
import pandas as pd
import yaml  # type: ignore

from tax_loss.db import convert_trade
from tax_loss.trade import Side


def build_query(table_name: str, start_ts: int) -> str:
    return f"SELECT * FROM {table_name} WHERE (exchange_ts >= {start_ts}) AND (side == 'SELL') ORDER BY exchange_ts ASC"


@click.command()
@click.option("--config", "config_file", required=True)
def main(config_file: str) -> None:
    config = munch.munchify(yaml.safe_load(open(config_file)))
    con = sqlite3.connect(config.database.file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    table_name: str = config.database.trades_table_name

    wash_days = config.wash_sale_days
    start_date = pd.Timestamp.now().date() - pd.Timedelta(f"{wash_days} days")
    start_ts = pd.Timestamp(start_date).value

    cur.execute(build_query(table_name, start_ts))
    ticker_to_expiry_date: Dict[str, datetime.date] = {}
    while trade_db := cur.fetchone():
        trade = convert_trade(trade_db)
        if trade.side == Side.SELL and trade.exchange_ts is not None:
            ticker_to_expiry_date[trade.symbol] = trade.exchange_ts.date() + pd.Timedelta(f"{wash_days} days")

    print(ticker_to_expiry_date)
    jsonable = {t: (str(v) if v is not None else v) for t, v in ticker_to_expiry_date.items()}
    with open(config.ticker_blacklist_file, "w") as f:
        json.dump(jsonable, f, indent=2)


if __name__ == "__main__":
    sys.exit(main())
