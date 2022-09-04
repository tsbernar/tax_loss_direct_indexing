import sys

import click
import munch
import pandas as pd
import yaml  # type: ignore

from tax_loss.gateway import IBKRGateway
from tax_loss.portfolio import Portfolio
from tax_loss.strategy import DirectIndexTaxLossStrategy


@click.command()
@click.option("--config", "config_file", required=True)
def main(config_file):
    config = yaml.safe_load(open(config_file))
    config = munch.munchify(config)
    gw = IBKRGateway(config=config.gateway)
    pf = gw.get_current_portfolio()
    print(pf.head(None, loss_sorted=False))
    print(f"Account ID: {gw.account_id}")
    if click.confirm("Confirm liquidation"):
        empty_pf = Portfolio.from_weights(
            weights=pd.Series(), nav=pf.nav, ticker_to_market_price=pf.ticker_to_market_price
        )
        trades = DirectIndexTaxLossStrategy._plan_transactions(
            "dummy", desired_portfolio=empty_pf, current_portfolio=pf
        )
        executed_trades = gw.try_execute(trades)
        print(executed_trades)


if __name__ == "__main__":
    sys.exit(main())
