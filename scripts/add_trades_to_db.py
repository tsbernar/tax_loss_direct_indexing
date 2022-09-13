import json
import sqlite3
import sys
from decimal import Decimal

import click
import pandas as pd
import urllib3

from tax_loss.gateway import IBKRGateway
from tax_loss.trade import Side, Trade
from tax_loss.util import read_config

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def build_check_all_query(table_name: str) -> str:
    return f"SELECT * FROM {table_name}"


def build_check_query(col_name: str, value: str, table_name: str) -> str:
    return f"SELECT {col_name} FROM {table_name} WHERE {col_name}='{value}' LIMIT 1"


def build_insert_ibkr_statement(table_name: str) -> str:
    return f"INSERT INTO {table_name} VALUES(?, ?)"


def build_create_ibkr_statement(table_name: str) -> str:
    return f"CREATE TABLE IF NOT EXISTS {table_name} (execution_id UNIQUE NOT NULL PRIMARY KEY, json)"


def build_create_trades_statement(table_name: str) -> str:
    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (exchange_trade_id UNIQUE NOT NULL PRIMARY KEY, "
        f"symbol, qty, price, side, fee, exchange_symbol, create_ts, exchange_ts, order_id, id); "
    )


def decode_ibkr_trade(ibkr_trade) -> Trade:
    if ibkr_trade["side"] == "B":
        side = Side.BUY
    elif ibkr_trade["side"] == "S":
        side = Side.SELL
    else:
        side = Side.UNKNOWN
    trade = Trade(
        symbol=str(ibkr_trade["symbol"]),  # TODO map this?
        qty=Decimal(str(ibkr_trade["size"])),
        price=Decimal(str(ibkr_trade["price"])),
        side=side,
        fee=Decimal(str(ibkr_trade["commission"])),
        exchange_symbol=str(ibkr_trade["conid"]),
        exchange_ts=pd.Timestamp(ibkr_trade["trade_time_r"], unit="ms", tz="UTC").tz_convert("America/Chicago"),
        exchange_trade_id=str(ibkr_trade["execution_id"]),
        order_id=str(ibkr_trade["order_ref"]) if "order_ref" in ibkr_trade else None,
    )
    return trade


def build_insert_trades_statement(table_name: str) -> str:
    return f"INSERT INTO {table_name} VALUES(?,?,?,?,?,?,?,?,?,?,?)"


@click.command()
@click.option("--config", "config_file", required=True)
@click.option("-v", "--verbose", is_flag=True)
@click.option("--create_tables", is_flag=True)
@click.option("--convert_all", is_flag=True)
def main(config_file: str, verbose: bool, create_tables: bool, convert_all: bool) -> None:
    config = read_config(config_file)
    ibkr_table_name = config.database.ibkr_trades_json_table_name
    trade_table_name = config.database.trades_table_name

    con = sqlite3.connect(config.database.file)
    cur = con.cursor()

    if create_tables:
        cur.execute(build_create_ibkr_statement(ibkr_table_name))
        con.commit()
        cur.execute(build_create_trades_statement(trade_table_name))
        con.commit()

    gw = IBKRGateway(config=config.gateway)
    ibkr_trades = gw.get_ibkr_trades()
    inserted = {"trades": 0, "ibkr": 0}

    for ibkr_trade in ibkr_trades:
        if verbose:
            print(ibkr_trade)
        execution_id = ibkr_trade["execution_id"]
        cur.execute(build_check_query("execution_id", str(execution_id), ibkr_table_name))
        if cur.fetchone() is None:
            cur.execute(build_insert_ibkr_statement(ibkr_table_name), (execution_id, json.dumps(ibkr_trade)))
            con.commit()
            inserted["ibkr"] += 1
            if verbose:
                print("Inserting to ibkr table")
        cur.execute(build_check_query("exchange_trade_id", str(execution_id), trade_table_name))
        if cur.fetchone() is None:
            trade = decode_ibkr_trade(ibkr_trade)
            cur.execute(
                build_insert_trades_statement(trade_table_name),
                (
                    str(trade.exchange_trade_id),
                    str(trade.symbol),
                    str(trade.qty),
                    str(trade.price),
                    str(trade.side.name),
                    str(trade.fee),
                    str(trade.exchange_symbol),
                    trade.create_ts.value,
                    pd.Timestamp(trade.exchange_ts).value,
                    str(trade.order_id),
                    str(trade.id),
                ),
            )
            con.commit()
            inserted["trades"] += 1
            if verbose:
                print("Inserting to trades table")

    converted = 0
    if convert_all:
        cur2 = con.cursor()
        cur.execute(build_check_all_query(ibkr_table_name))
        while ibkr_trade := cur.fetchone():
            trade = decode_ibkr_trade(json.loads(ibkr_trade[1]))
            try:
                cur2.execute(
                    build_insert_trades_statement(trade_table_name),
                    (
                        str(trade.exchange_trade_id),
                        str(trade.symbol),
                        str(trade.qty),
                        str(trade.price),
                        str(trade.side.name),
                        str(trade.fee),
                        str(trade.exchange_symbol),
                        pd.Timestamp(trade.create_ts).value,
                        pd.Timestamp(trade.exchange_ts).value,
                        str(trade.order_id),
                        str(trade.id),
                    ),
                )
                con.commit()
                converted += 1
            except sqlite3.IntegrityError:
                pass  # already exists

    print(
        f"Inserted {inserted['ibkr']} trades to ibkr table, skipped {len(ibkr_trades) - inserted['ibkr']} already in db"
    )
    print(f"Inserted {inserted['trades']} trades to trades table, skipped {len(ibkr_trades) - inserted['trades']}")
    if convert_all:
        print(f"Converted {converted} additional trades from ibkr to trades table")


if __name__ == "__main__":
    sys.exit(main())
