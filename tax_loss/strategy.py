import datetime
import json
import logging
from typing import Dict, List, Optional, Tuple

import munch
import pandas as pd

from .optimizer import IndexOptimizer, MinimizeOptimizer
from .portfolio import Portfolio

logger = logging.getLogger(__name__)


class DirectIndexTaxLossStrategy:
    def __init__(self, config: munch.Munch) -> None:
        self.config = config
        self.current_portfolio: Portfolio = Portfolio(filename=config.portfolio_file)
        self.ticker_blacklist: List[str] = self._load_ticker_blacklist(config.ticker_blacklist_file, config)
        self.index_weights = self._load_index_weights(config.index_weight_file, config.max_stocks)
        self.price_matrix = self._load_yf_prices(config.price_data_file, config.optimizer.lookback_days)
        self.optimizer = self._init_optimzier(config.optimizer)

    def run(self) -> None:
        logger.info("Running optimization")
        weights, result = self.optimizer.optimize()
        logger.info(f"Weights: {weights}")
        desired_portfolio = Portfolio.from_weights(
            weights=weights,
            nav=self.current_portfolio.nav,
            ticker_to_market_price=self.current_portfolio.ticker_to_market_price,
        )
        logger.info(f"Desired portfolio: {desired_portfolio}")
        # desired_transactions = transaction_planner(current_portfolio, desired_portfolio)
        # transaction results = gateway(desired_transactions)
        # current_portfolio = f(current_portfolio, transaction_results)
        # blacklist additions = f(transaction results)
        # save data (current porfolio, blacklist)
        # pull IBKR pf data, sanity check vs current pf?

    def _load_ticker_blacklist(self, filename: str, config: munch.Munch) -> List[str]:
        logger.debug(f"Loading ticker blacklist from {filename}")
        with open(filename) as f:
            ticker_to_expiry: Dict[str, Optional[str]] = json.loads(f.read())

        ticker_blacklist = []
        for ticker, expiry_str in ticker_to_expiry.items():
            if expiry_str is None:
                ticker_blacklist.append(ticker)
            elif pd.to_datetime(expiry_str).date() < datetime.date.today():
                ticker_blacklist.append(ticker)

        logger.debug(f"Adding extra ticker blacklist from config: {config.ticker_blacklist_extra}")
        ticker_blacklist = ticker_blacklist + config.ticker_blacklist_extra
        logger.info(f"Ticker blacklist: {ticker_blacklist}")
        return ticker_blacklist

    def _load_yf_prices(self, filename: str, lookback_days: int) -> pd.DataFrame:
        start_date = datetime.date.today() - pd.Timedelta(f"{lookback_days} days")
        logger.debug(f"Loading yf price data from {start_date}")
        price_matrix = pd.read_parquet(filename)
        price_matrix = price_matrix[start_date:].dropna(axis=1)
        logger.info(f"Price matrix: {price_matrix}")
        return price_matrix

    def _load_index_weights(self, filename: str, max_stocks: int) -> pd.Series:
        # returns a series with ticker as index and weight as values
        logger.debug(f"Loading index weights from {filename}, max_stocks: {max_stocks}")
        df = pd.read_parquet(filename)
        # we only care about the lastest weights.. TODO: keep a seperate file cached with just latest date?
        date = df.date.max()
        weights = df[df.date == date].groupby(["Ticker"])["Weight (%)"].sum().reset_index()
        weights = weights.sort_values("Weight (%)").tail(max_stocks)
        weights = weights.set_index("Ticker")["Weight (%)"].sort_index()
        weights = weights / 100  # convert from % value
        weights["IVV"] = 0

        logger.info(f"Loaded {len(weights)} index weights from {date}")
        return weights

    def _make_index_returns(self, ticker: str) -> pd.DataFrame:
        return self.price_matrix[ticker].pct_change().tail(-1)

    def _make_component_returns(self) -> pd.DataFrame:
        return self.price_matrix.pct_change().tail(-1)

    def _drop_missing_tickers(
        self, component_returns: pd.DataFrame, index_weights: pd.Series
    ) -> Tuple[pd.DataFrame, pd.Series]:
        missing_tickers = [m for m in index_weights.index if m not in component_returns.columns]
        logger.info(f"Dropping tickers witih missing price data: {missing_tickers}")
        tickers = index_weights.index.drop(missing_tickers)
        component_returns = component_returns[tickers]
        index_weights = index_weights[tickers]
        return component_returns, index_weights

    def _init_optimzier(self, config: munch.Munch) -> IndexOptimizer:
        logger.info("Initializing optimizer")
        index_returns = self._make_index_returns("IVV")
        component_returns = self._make_component_returns()
        #  TODO add 0 weight for positions currently in pf but that are dropped from index?
        # So we don't have to immediately sell when they are dropped
        true_index_weights = self.index_weights
        component_returns, true_index_weights = self._drop_missing_tickers(component_returns, true_index_weights)

        tax_coefficient = float(config.tax_coefficient)
        starting_portfolio = self.current_portfolio
        initial_weight_guess = self.index_weights  # TODO (guess current pf or true weights if current is too far off?)
        max_deviation_from_true_weight = float(config.max_deviation_from_true_weight)

        # Do not increase posiiton for tickers in the blacklist
        ticker_blacklist = {ticker: (0.0, starting_portfolio.weight(ticker)) for ticker in self.ticker_blacklist}
        # Cap IVV weight to 10%  TODO: handle flipping between SPY, etc.
        if "IVV" not in ticker_blacklist:
            ticker_blacklist["IVV"] = (0.0, 0.1)
        # Cant have bounds where min == max, so set 0 max to something small that will round to 0 shares.
        for ticker, weight_range in ticker_blacklist.items():
            if weight_range[0] == weight_range[1]:
                ticker_blacklist[ticker] = (weight_range[0], weight_range[1] + 1e-6)

        cash_constraint = config.cash_constraint
        tracking_error_func = config.tracking_error_func
        optimizer = MinimizeOptimizer(
            index_returns=index_returns,
            component_returns=component_returns,
            true_index_weights=true_index_weights,
            tax_coefficient=tax_coefficient,
            starting_portfolio=starting_portfolio,
            initial_weight_guess=initial_weight_guess,
            max_deviation_from_true_weight=max_deviation_from_true_weight,
            ticker_blacklist=ticker_blacklist,
            cash_constraint=cash_constraint,
            tracking_error_func=tracking_error_func,
        )
        return optimizer
