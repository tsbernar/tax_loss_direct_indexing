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


@click.command()
@click.option("--config", "config_file", required=True)
@click.option("-v", "--verbose")
def main(config_file, verbose):
    config = munch.munchify(yaml.safe_load(open(config_file)))

    con = sqlite3.connect(config.database.file)
    cur = con.cursor()

    gw = IBKRGateway(config=config.gateway)
    ibkr_trades = gw.get_ibkr_trades()
    inserted = 0

    for trade in ibkr_trades:
        if verbose:
            print(trade)
        execution_id = trade["execution_id"]
        cur.execute(build_check_query(execution_id, config.database.ibkr_trades_json_table_name))
        if cur.fetchone() is None:
            print(f"Inserting trade {trade}")
            cur.execute(
                build_insert_statement(config.database.ibkr_trades_json_table_name), (execution_id, json.dumps(trade))
            )
            con.commit()
            inserted += 1
            if verbose:
                print("Inserting")

    print(f"Inserted {inserted} trades, skipped {len(ibkr_trades) - inserted} already in db")


if __name__ == "__main__":
    sys.exit(main())

