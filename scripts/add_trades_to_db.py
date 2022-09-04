import json
import sqlite3
import sys

import click
import munch
import yaml  # type: ignore

from tax_loss.gateway import IBKRGateway
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 


def build_check_query(execution_id: str, table_name: str) -> str:
    return f"SELECT execution_id from {table_name} WHERE execution_id='{execution_id}' LIMIT 1"


def build_insert_statement(table_name: str) -> str:
    return f"INSERT INTO {table_name} VALUES(?, ?)"


def build_create_statement(table_name: str) -> str:
    return f"CREATE TABLE {table_name}(execution_id UNIQUE, json)"


@click.command()
@click.option("--config", "config_file", required=True)
@click.option("-v", "--verbose", is_flag=True)
@click.option("--create_table", is_flag=True)
def main(config_file: str, verbose: bool, create_table: bool) -> None:
    config = munch.munchify(yaml.safe_load(open(config_file)))
    table_name = config.database.ibkr_trades_json_table_name

    con = sqlite3.connect(config.database.file)
    cur = con.cursor()

    if create_table:
        cur.execute(build_create_statement(table_name))
        con.commit()

    gw = IBKRGateway(config=config.gateway)
    ibkr_trades = gw.get_ibkr_trades()
    inserted = 0

    for trade in ibkr_trades:
        if verbose:
            print(trade)
        execution_id = trade["execution_id"]
        cur.execute(build_check_query(execution_id, table_name))
        if cur.fetchone() is None:
            print(f"Inserting trade {trade}")
            cur.execute(
                build_insert_statement(table_name), (execution_id, json.dumps(trade))
            )
            con.commit()
            inserted += 1
            if verbose:
                print("Inserting")

    print(f"Inserted {inserted} trades, skipped {len(ibkr_trades) - inserted} already in db")


if __name__ == "__main__":
    sys.exit(main())

