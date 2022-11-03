import os
import sys
from typing import Dict, Optional

import click
import pandas as pd  # type: ignore


def parse_ts(line: str) -> pd.Timestamp:
    return pd.to_datetime(line[1:].split("]")[0])


def get_last_ts(filename: str) -> Optional[pd.Timestamp]:
    if not os.path.exists(filename):
        return None
    with open(filename) as f:
        line = None
        for line in f:
            pass
        if line is not None:
            return pd.to_datetime(line.split(",")[0])


def write(data: Dict[pd.Timestamp, float], filename: str) -> None:
    with open(filename, "a+") as f:
        for t, v in data.items():
            f.write(f"{t},{v}\n")


@click.command()
@click.option("--logfile", required=True)
@click.option("--navout", default="nav.csv")
@click.option("--cashout", default="cash.csv")
def main(logfile: str, navout: str, cashout: str):
    with open(logfile) as f:
        ts_to_nav = {}
        ts_to_cash_change = {}
        for line in f:
            if "Current portfolio nav:" in line:
                ts_to_nav[parse_ts(line)] = float(
                    line.split("Current portfolio nav:")[1]
                )
            if "Updating cash by $" in line:
                ts_to_cash_change[parse_ts(line)] = float(
                    line.split("Updating cash by $")[1]
                )

    # If we already have data saved, just append new data
    last_ts_nav = get_last_ts(navout)
    last_ts_cash_change = get_last_ts(cashout)
    if last_ts_nav:
        ts_to_nav = {ts: nav for ts, nav in ts_to_nav.items() if ts > last_ts_nav}
    if last_ts_cash_change:
        ts_to_cash_change = {
            ts: cash
            for ts, cash in ts_to_cash_change.items()
            if ts > last_ts_cash_change
        }
    print(f"Found {len(ts_to_nav)} nav updates.")
    print(f"Found {len(ts_to_cash_change)} cash changes.")
    if ts_to_nav:
        write(ts_to_nav, navout)
    if ts_to_cash_change:
        write(ts_to_cash_change, cashout)


if __name__ == "__main__":
    sys.exit(main())
