import datetime
import json
import logging
from typing import Dict, List, Optional

import munch
import pandas as pd
from portfolio import Portoflio

logger = logging.getLogger(__name__)


class DirectIndexTaxLossStrategy:
    def __init__(self, config: munch.Munch) -> None:
        self.config = config
        self.current_portfolio: Portoflio = Portoflio.from_json_file(config.portfolio_file)
        self.ticker_blacklist: List[str] = self.load_ticker_blacklist(config.ticker_blacklist_file)
        self.index_weights = self.load_index_weights(config.index_weight_file, config.max_stocks)

    def load_ticker_blacklist(self, filename: str) -> List[str]:
        with open(filename) as f:
            ticker_to_expiry: Dict[str, Optional[str]] = json.loads(f.read())

        ticker_blacklist = []
        for ticker, expiry_str in ticker_to_expiry.items():
            if expiry_str is None:
                ticker_blacklist.append(ticker)
            elif pd.to_datetime(expiry_str).date() < datetime.date.today():
                ticker_blacklist.append(ticker)

        return ticker_blacklist

    def load_index_weights(self, filename: str, max_stocks: int) -> pd.DataFrame:
        df = pd.read_parquet(filename)
        # we only care about the lastest weights.. TODO: keep a seperate file cached with just latest date?
        date = df.date.max()
        df = df[df.date == date].groupby(["Ticker"])["Weight (%)"].sum().reset_index()
        df = df.sort_values("Weight (%)").tail(max_stocks)
        df = df.set_index("Ticker")["Weight (%)"].sort_index()

        logger.info(f"Loaded {len(df)} index weights from {date}")
        return df

    def run(self) -> None:
        pass
