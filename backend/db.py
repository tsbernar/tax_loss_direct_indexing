import sqlite3
from decimal import Decimal
from typing import List, Optional

import munch
import pandas as pd

from tax_loss.trade import Side, Trade


def convert_trade(row: sqlite3.Row) -> Trade:
    return Trade(
        symbol=row["symbol"],
        qty=Decimal(row["qty"]),
        price=Decimal(row["price"]),
        side=Side[row["side"]],
        fee=Decimal(row["fee"]),
        exchange_symbol=row["exchange_symbol"],
        exchange_ts=pd.Timestamp(row["exchange_ts"]),
        exchange_trade_id=row["exchange_trade_id"],
        order_id=row["order_id"],
        id=row["id"],
    )


def get_trades(config: munch.Munch, start_ts: Optional[pd.Timestamp] = None) -> List[Trade]:
    table_name: str = config.database.trades_table_name
    if start_ts is None:
        start_ts = pd.Timestamp(0)

    query = f"SELECT * FROM {table_name} WHERE (exchange_ts >= {start_ts.value}) ORDER BY exchange_ts ASC"

    con = sqlite3.connect(config.database.file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute(query)

    trades = []
    while trade_db := cur.fetchone():
        trade = convert_trade(trade_db)
        trades.append(trade)

    return trades
