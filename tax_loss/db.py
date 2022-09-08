import sqlite3
from decimal import Decimal

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
