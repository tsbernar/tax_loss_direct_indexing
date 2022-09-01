import datetime
import json
import logging
import time
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

import munch
import pandas as pd

from .optimizer import IndexOptimizer, MinimizeOptimizer
from .portfolio import MarketPrice, Portfolio
from .trade import Side, Trade

INDEX_TICKER = "IVV"
DRY_RUN = "dry_run"
PX_PRECISION = "0.01"
logger = logging.getLogger(__name__)


class DirectIndexTaxLossStrategy:
    def __init__(self, config: munch.Munch) -> None:
        self.config = config
        self.price_matrix = self._load_yf_prices(config.price_data_file, config.optimizer.lookback_days)
        self.current_portfolio = self._load_current_portfolio(config.portfolio_file)
        self._update_market_prices()
        self.ticker_blacklist: List[str] = self._load_ticker_blacklist(config.ticker_blacklist_file, config)
        self.index_weights = self._load_index_weights(config.index_weight_file, config.max_stocks)
        self.optimizer = self._init_optimzier(config.optimizer)

    def run(self) -> None:
        logger.info("Running optimization")
        t0 = time.time()
        weights, result = self.optimizer.optimize()
        elapsed = time.time() - t0
        logger.info(f"Optimization took {elapsed : .2f}s")
        if not result.success:
            logger.critical(f"Failed optimization.. result: {result}")
        logger.info(f"Weights:\n{weights.sort_values()}")
        logger.info(f"Total deviation from index: {abs(weights - self.index_weights).sum()}")
        logger.info(f"Total weight: {weights.sum()}")
        logger.debug(f"Current pf nav: {self.current_portfolio.nav}")
        desired_portfolio = Portfolio.from_weights(
            weights=weights,
            nav=self.current_portfolio.nav,
            ticker_to_market_price=self.current_portfolio.ticker_to_market_price,
        )
        logger.info(f"Desired portfolio:\n{desired_portfolio}")
        desired_trades = self._plan_transactions(
            desired_portfolio=desired_portfolio, current_portfolio=self.current_portfolio
        )
        logger.debug(f"len desired_trades: {len(desired_trades)}")
        logger.info(f"Desired trades:\n{chr(10).join(map(str,desired_trades))}")

        if DRY_RUN in self.config:
            logger.info(f"Saving desired portfolio to {self.config[DRY_RUN].desired_portfolio_file}")
            desired_portfolio.to_json_file(self.config[DRY_RUN].desired_portfolio_file)
            if self.config[DRY_RUN].rotate_desired_current:
                filename = self.config.portfolio_file + pd.Timestamp.now().strftime("%Y%m%d_%H%M")
                if ".json" in filename:  # move extension to the end
                    filename = filename.replace(".json", "") + ".json"
                logger.info(f"Rotating last portfolio to {filename}")
                self.current_portfolio.to_json_file(filename=filename)
                # for dry run, assume we can execute all trades at current prices
                logger.info("Updating portfolio with trades")
                self.current_portfolio.update(desired_trades)
                logger.info(f"Current portfolio: {self.current_portfolio}")
                self.current_portfolio.to_json_file(filename=self.config.portfolio_file)

        # transaction results = gateway(desired_transactions)
        # current_portfolio = f(current_portfolio, transaction_results)
        # blacklist additions = f(transaction results)
        # save data (current porfolio, blacklist)
        # pull IBKR pf data, sanity check vs current pf?

    def _load_current_portfolio(self, filename: str) -> Portfolio:
        logger.info(f"Loading current portfolio from {filename}")
        current_portfolio = Portfolio(filename=filename)
        logger.debug(f"Current portfolio: {current_portfolio}")
        return current_portfolio

    def _update_market_prices(self) -> None:
        logger.debug("Updating market prices")
        latest_date: pd.Timestamp = self.price_matrix.index.max()  # TODO : update with IBKR live prices
        latest_prices = self.price_matrix.loc[latest_date]
        to_update = {t: MarketPrice(v, latest_date.to_pydatetime()) for t, v in latest_prices.items()}
        self.current_portfolio.ticker_to_market_price.update(to_update)
        logger.info(f"Current portfolio: {self.current_portfolio}")

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

        logger.info(f"Adding extra ticker blacklist from config: {config.ticker_blacklist_extra}")
        ticker_blacklist = ticker_blacklist + config.ticker_blacklist_extra
        logger.info(f"Ticker blacklist: {ticker_blacklist}")
        return ticker_blacklist

    def _load_yf_prices(self, filename: str, lookback_days: int) -> pd.DataFrame:
        start_date = datetime.date.today() - pd.Timedelta(f"{lookback_days} days")
        logger.info(f"Loading yf price data from {start_date}")
        price_matrix = pd.read_parquet(filename)
        price_matrix = price_matrix[start_date:].dropna(axis=1)
        logger.debug(f"Price matrix:\n{price_matrix}")
        return price_matrix

    def _load_index_weights(self, filename: str, max_stocks: int) -> pd.Series:
        # returns a series with ticker as index and weight as values
        logger.info(f"Loading index weights from {filename}, max_stocks: {max_stocks}")
        df = pd.read_parquet(filename)
        # we only care about the lastest weights.. TODO: keep a seperate file cached with just latest date?
        date = df.date.max()
        weights = df[df.date == date].groupby(["Ticker"])["Weight (%)"].sum().reset_index()
        weights = weights.sort_values("Weight (%)").tail(max_stocks)
        weights = weights.set_index("Ticker")["Weight (%)"].sort_index()
        weights = weights / 100  # convert from % value
        weights[INDEX_TICKER] = 0

        logger.info(f"Loaded {len(weights)} index weights from {date}")
        logger.debug(weights)
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

    def _plan_transactions(self, desired_portfolio: Portfolio, current_portfolio: Portfolio) -> List[Trade]:
        logger.info("Planning transactins")
        tax_gain = 0.0
        desired_tickers = set(desired_portfolio.ticker_to_cost_basis.keys())
        current_tickers = set(current_portfolio.ticker_to_cost_basis.keys())
        trades = []

        for ticker in desired_tickers.union(current_tickers):
            current_qty = Decimal()
            desired_qty = Decimal()
            if ticker in current_portfolio.ticker_to_cost_basis:
                current_qty = current_portfolio.ticker_to_cost_basis[ticker].total_shares
            if ticker in desired_portfolio.ticker_to_cost_basis:
                desired_qty = desired_portfolio.ticker_to_cost_basis[ticker].total_shares

            trade_qty = desired_qty - current_qty

            if trade_qty == 0:
                logger.info(f"No trade for {ticker}")
                continue

            # TODO maybe assume some slippage when making desired pf?
            trade_px = Decimal(desired_portfolio.ticker_to_market_price[ticker].price)
            trade_px = trade_px.quantize(Decimal(PX_PRECISION))
            side = Side.BUY if trade_qty > 0 else Side.SELL
            trade = Trade(qty=abs(trade_qty), price=trade_px, side=side, symbol=ticker)
            trades.append(trade)

            if side == Side.SELL:
                total_basis = current_portfolio.ticker_to_cost_basis[ticker].hifo_basis(-trade_qty)
                assert total_basis.shares == -trade_qty
                tax_gain += float(-trade_qty) * (float(trade_px) - total_basis.price)

        logger.info(f"Planned tax gain of ${tax_gain : .2f}")

        return trades

    def _init_optimzier(self, config: munch.Munch) -> IndexOptimizer:
        logger.info("Initializing optimizer")
        index_returns = self._make_index_returns(INDEX_TICKER)
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
        if INDEX_TICKER not in ticker_blacklist:
            ticker_blacklist[INDEX_TICKER] = (0.0, 0.1)
        # Cant have bounds where min == max, so set 0 max to something small that will round to 0 shares.
        for ticker, weight_range in ticker_blacklist.items():
            if weight_range[0] == weight_range[1]:
                ticker_blacklist[ticker] = (weight_range[0], weight_range[1] + 1e-6)

        cash_constraint = config.cash_constraint
        tracking_error_func = config.tracking_error_func
        max_total_deviation = config.max_total_deviation

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
            max_total_deviation=max_total_deviation,
        )
        return optimizer
