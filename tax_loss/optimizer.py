import abc
import logging
from functools import partial
from time import time
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy.optimize import OptimizeResult, minimize

from .portfolio import Portfolio

logger = logging.getLogger(__name__)


class IndexOptimizer(abc.ABC):
    def __init__(self, index_returns, component_returns):
        """
        index_returns:
            Vector of index returns per date.  Optional date index.
            Ex:

            Date
            2021-01-04   -0.013772
            2021-01-05    0.006618
            ...

        component_returns:
            Matrix of returns of some stocks to be used to replicate the index.  Optional date index.
            Ex:

            Date            AAPL      ABBV       ABT
            2021-01-04 -0.024719 -0.016239 -0.003471
            2021-01-05  0.012364  0.010341  0.012373

        """
        self.index_returns = index_returns
        self.component_returns = component_returns

    @abc.abstractmethod
    def optimize(self):
        """
        Returns:

            weights: pd.Series
                weights of component returns.
                Ex:

                AMZN    0.0435
                MSFT    0.0526
                AAPL    0.0661

            full_result: Any
                Full native result from optimzer.

        """
        pass


class MinimizeOptimizer(IndexOptimizer):
    def __init__(
        self,
        index_returns: pd.Series,
        component_returns: pd.DataFrame,
        true_index_weights: pd.Series,
        tax_coefficient: float,
        starting_portfolio: Portfolio,
        initial_weight_guess: pd.Series,
        max_deviation_from_true_weight: float = 0.03,
        ticker_blacklist: Dict[str, Tuple[float, float]] = None,
        cash_constraint: float = 0.95,
        tracking_error_func: str = "least_squared",
        max_total_deviation: float = 0.8,
    ):
        # TODO: make max_total_deviation and tax_coefficient optional..
        # can significanlty speed up optimiziation if not provided
        self.index_returns = index_returns
        self.component_returns = component_returns
        self.true_index_weights = true_index_weights
        self.tax_coefficient = tax_coefficient
        self.starting_portfolio = starting_portfolio
        self.initial_weight_guess = initial_weight_guess
        self.ticker_blacklist = ticker_blacklist if ticker_blacklist is not None else {}
        self.max_deviation_from_true_weight = max_deviation_from_true_weight
        self.cash_constraint = cash_constraint
        self.max_total_deviation = max_total_deviation

        if tracking_error_func == "least_squared":
            self.tracking_error_func = self._least_squared
        elif tracking_error_func == "var_tracking_diff":
            self.tracking_error_func = self._var_tracking_diff
        else:
            raise ValueError(f"Unrecognized tracking_error_func: {self.tracking_error_func}")

    def _get_bounds(self, ticker, tw):
        if ticker in self.ticker_blacklist:
            return self.ticker_blacklist[ticker]  # TODO we should keep the min here even if in blacklist in some cases
        min_bound = max(0, tw - self.max_deviation_from_true_weight)
        max_bound = tw + self.max_deviation_from_true_weight
        return (min_bound, max_bound)

    @staticmethod
    def _tax_loss_pct_harvested(
        weights,
        ticker_indices,
        starting_portfolio,
        starting_portfolio_weights,
        starting_portfolio_prices,
        starting_portfolio_nav,
    ):
        # Tax loss is positive number, tax gain is negative number
        # returns tax losses harvested as a percentage of total portfolio value
        # TODO: later add option to optimize around fixed value harvested losses rather than portfolio size?
        # to keep more in line with expected taxable income rather than pf size which could be much bigger.

        total_tax_loss = 0
        hifo_time = 0

        share_changes = (weights - starting_portfolio_weights) / starting_portfolio_prices * starting_portfolio_nav
        share_changes = pd.DataFrame(
            {
                "share_change": share_changes,
                "ticker": ticker_indices,
                "market_price": starting_portfolio_prices,
            }
        )
        share_sells = share_changes[share_changes.share_change < 0]

        if len(share_sells):
            # THIS IS A BOTTLENECK, precompute hifo price and sahres for fixed share increments and look up in a matrix?
            t0 = time()
            hifo_basis_prices = share_sells.apply(
                lambda x: starting_portfolio.ticker_to_cost_basis[x.ticker].hifo_basis(-x.share_change).price,
                axis=1,
            )
            hifo_time = time() - t0

            tax_loss = (share_sells.market_price - hifo_basis_prices) * share_sells.share_change
            total_tax_loss = tax_loss.sum()

        return total_tax_loss / starting_portfolio_nav, hifo_time

    @staticmethod
    def _least_squared(index_returns, component_returns, weights):
        return np.square((index_returns - component_returns.dot(weights)) * 100).mean()

    @staticmethod
    def _var_tracking_diff(index_returns, component_returns, weights):
        tracking_diff = (index_returns - component_returns.dot(weights)) * 100
        return np.var(tracking_diff)

    @staticmethod
    def x_to_weights(x: np.ndarray) -> np.ndarray:
        # 1st half of input is weights, 2nd half is extra vars for index drift constraints
        return x[: len(x) // 2]

    @staticmethod
    def x_to_extra_vars(x: np.ndarray) -> np.ndarray:
        # 1st half of input is weights, 2nd half is extra vars for index drift constraints
        return x[len(x) // 2 :]

    def _make_cash_constraints(self) -> List[Dict[str, Any]]:
        # must use at least cash_constraint% of cash (this constraint functions should return >=0 when condition met)
        # can't use more than available cash (this constraint functions should return >=0 when condition met)
        cons = [
            {"type": "ineq", "fun": lambda x: self.x_to_weights(x).sum() - self.cash_constraint},
            {"type": "ineq", "fun": lambda x: 1 - self.x_to_weights(x).sum()},
        ]
        return cons

    def _make_index_drift_constraints(self) -> List[Dict[str, Any]]:
        # constraint for sum(abs(weights - true_index_weights)) < index_drift_constraint
        # because we can't use absolute values in optimization, we need to restate the constraints with extra variables
        # abs(a) + abs(b) < c is equivilant to:
        # x + y = c
        # x - a > c; x + a > 0
        # y - b > c; y + b < 0
        # https://optimization.cbe.cornell.edu/index.php?title=Optimization_with_absolute_values#Application_in_Financial:_Portfolio_Selection
        # https://stackoverflow.com/questions/29795632/sum-of-absolute-values-constraint-in-semi-definite-programming
        def drift_constraint(x: np.ndarray, ix: int, true_index_weights: pd.Series):
            ei = self.x_to_extra_vars(x)[ix]
            wi = self.x_to_weights(x)[ix]
            ci = true_index_weights[ix]
            return min(ei - (wi - ci), ei + (wi - ci))

        cons = [{"type": "eq", "fun": lambda x: self.x_to_extra_vars(x).sum() - self.max_total_deviation}]
        for i in range(len(self.true_index_weights)):
            cons.append(
                {"type": "ineq", "fun": partial(drift_constraint, ix=i, true_index_weights=self.true_index_weights)}
            )
        return cons

    def _score(
        self,
        x: np.ndarray,
        ticker_indices: List[str],
        times: List[float],
        starting_portfolio_weights: List[float],
        starting_portfolio_prices: List[float],
        starting_portfolio_nav: float,
    ):
        # TODO clean up this funciton now that it no longer needs to be static should access self. more and not pass so
        # many args
        weights = self.x_to_weights(x)

        t0 = time()
        tracking_error = self.tracking_error_func(self.index_returns, self.component_returns, weights)
        times[0] += time() - t0
        t0 = time()
        tax_loss_harvested, hifo_time = MinimizeOptimizer._tax_loss_pct_harvested(
            weights,
            ticker_indices,
            self.starting_portfolio,
            starting_portfolio_weights,
            starting_portfolio_prices,
            starting_portfolio_nav,
        )
        times[1] += time() - t0
        times[2] += hifo_time
        times[3] += 1

        if not times[3] % 500:

            logger.debug(
                f"tracking_error: {tracking_error : .4f}, "
                f"tax_loss_harvested: {tax_loss_harvested : .4f}, "
                f"tax score: {self.tax_coefficient*tax_loss_harvested : .4f}, "
                f"total score: {tracking_error - self.tax_coefficient*tax_loss_harvested : .4f}, "
                f"tracking_error_time: {times[0] : .2f}s, tax_loss_time: {times[1] : .2f}s, "
                f"hifo_time: {times[2]: .2f}s, count: {times[3]}"
            )
        return tracking_error - self.tax_coefficient * tax_loss_harvested

    def optimize(self) -> Tuple[pd.Series, OptimizeResult]:

        bounds = [self._get_bounds(ticker, tw) for ticker, tw in self.true_index_weights.iteritems()]
        cons = self._make_cash_constraints()
        # TODO add in extra penalty for churn
        # TODO some of this should be moved to init

        ticker_indices = list(self.initial_weight_guess.index)
        starting_portfolio_weights = [self.starting_portfolio.weight(ticker) for ticker in ticker_indices]
        starting_portfolio_prices = [
            self.starting_portfolio.ticker_to_market_price[ticker].price for ticker in ticker_indices
        ]
        starting_portfolio_nav = self.starting_portfolio.nav

        index_drift_extra_vars = pd.Series(
            [self.max_total_deviation / len(self.initial_weight_guess) for i in self.initial_weight_guess]
        )
        x0 = pd.concat([self.initial_weight_guess, index_drift_extra_vars])
        bounds += [(-np.inf, np.inf) for x in index_drift_extra_vars]  # no bounds on extra vars
        cons += self._make_index_drift_constraints()

        result = minimize(
            self._score,
            x0,
            args=(
                ticker_indices,
                [0.0, 0.0, 0.0, 0.0],
                starting_portfolio_weights,
                starting_portfolio_prices,
                starting_portfolio_nav,
            ),
            bounds=bounds,
            constraints=cons,
            options={"maxiter": 400}  # 4x the default max
            #  options={'eps':1e-8}
        )

        minimize_weights = pd.Series(self.x_to_weights(result.x), index=self.initial_weight_guess.index).sort_values()
        return minimize_weights, result
