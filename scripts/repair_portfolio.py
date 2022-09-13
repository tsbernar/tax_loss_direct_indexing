import sys
from typing import Optional

import click
import pandas as pd

from tax_loss.db import get_trades
from tax_loss.gateway import IBKRGateway
from tax_loss.portfolio import Portfolio
from tax_loss.util import read_config, repair_portfolio


@click.command()
@click.option("--config", "config_file", required=True)
@click.option("--after", required=False)
@click.option("--out_file", default="out_pf.json")
def main(config_file: str, after: Optional[str], out_file: str) -> None:
    config = read_config(config_file)
    gw = IBKRGateway(config.gateway)
    ib_pf = gw.get_current_portfolio()
    cache_pf = Portfolio(filename=config.portfolio_file)
    if after is None:
        after = "2022-09-01 10:00"
    trades = get_trades(config, start_ts=pd.Timestamp(after))
    if repaired := repair_portfolio(stale_portfolio=cache_pf, target_portfolio=ib_pf, trades=trades):
        print(f"Saving pf to {out_file}")
        repaired.to_json_file(filename=out_file)
    else:
        print("Could not repair pf")


if __name__ == "__main__":
    sys.exit(main())
