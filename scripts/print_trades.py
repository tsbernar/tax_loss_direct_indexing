import sqlite3
import sys
from typing import Optional

import click
import munch
import tabulate
import urllib3
import yaml  # type: ignore

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def build_check_query(
    table_name: str, verbose: bool, count: Optional[int] = None, order_by: Optional[str] = None
) -> str:
    col = "execution_id" if not verbose else "*"
    count = 1000 if count is None else count
    ret = f"SELECT {col} from {table_name}"
    if order_by:
        ret += f" ORDER BY {order_by} DESC"
    ret += f" LIMIT {count}"
    return ret


@click.command()
@click.option("--config", "config_file", required=True)
@click.option("-v", "--verbose", is_flag=True)
@click.option("-c", "--count", type=int)
def main(config_file: str, verbose: bool, count: int) -> None:
    config = munch.munchify(yaml.safe_load(open(config_file)))
    table_name = config.database.trades_table_name

    con = sqlite3.connect(config.database.file)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    print(table_name)
    cur.execute(build_check_query(table_name, verbose, count, "exchange_ts"))
    trades = []
    while trade := cur.fetchone():
        trades.append(dict(trade))

    print(tabulate.tabulate(trades, headers="keys"))


if __name__ == "__main__":
    sys.exit(main())
