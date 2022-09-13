import datetime
import logging
from typing import Any, Dict, List, Optional, Tuple, cast

import munch
import pandas as pd
import yaml  # type: ignore

from tax_loss.portfolio import Portfolio
from tax_loss.trade import Side, Trade

logger = logging.getLogger(__name__)


IBKR_DEFAULT_DAYS = {
    "20000101": "Saturday",
    "20000102": "Sunday",
    "20000103": "Monday",
    "20000104": "Tuesday",
    "20000105": "Wednesday",
    "20000106": "Thursday",
    "20000107": "Friday",
}


class Schedule:
    #  https://www.interactivebrokers.com/api/doc.html#tag/Contract/paths/~1trsrv~1secdef~1schedule/get

    def __init__(self, json_data: List[Dict[str, Any]], exchange_tz: str = "America/New_York"):
        self.tz = exchange_tz
        self.default_schedule = {}
        self.special_cases = {}
        for schedule in json_data:
            date = str(schedule["tradingScheduleDate"])
            if date in IBKR_DEFAULT_DAYS:
                self.default_schedule[IBKR_DEFAULT_DAYS[date]] = self._make_schedule(schedule["tradingtimes"])
            else:
                self.special_cases[pd.to_datetime(date).date()] = self._make_schedule(schedule["tradingtimes"])

    def is_open(self, ts: pd.Timestamp) -> bool:
        ts = ts.tz_convert(self.tz)
        date = ts.date()
        time = ts.time()
        if date in self.special_cases:
            open, close = self.special_cases[date]
        else:
            open, close = self.default_schedule[ts.day_name()]
        return (time > open) and (time < close)

    def _make_schedule(self, trading_times: List[Dict[str, str]]) -> Tuple[datetime.time, datetime.time]:
        open = trading_times[0]["openingTime"]
        close = trading_times[0]["closingTime"]
        open_ts = datetime.datetime.strptime(open, "%H%M").time()
        close_ts = datetime.datetime.strptime(close, "%H%M").time()
        return open_ts, close_ts


def read_config(filepath):
    config = yaml.safe_load(open(filepath))
    config = munch.munchify(config)
    return config


def _sorted_repair_portfolioo(
    stale_portfolio: Portfolio, target_portfolio: Portfolio, trades: List[Trade]
) -> Optional[Portfolio]:
    if stale_portfolio.positions == target_portfolio.positions:
        return stale_portfolio
    if not trades:
        return None
    trade = trades[0]
    del trades[0]
    if trade.side == Side.BUY:
        stale_portfolio.buy(trade.symbol, trade.qty, float(trade.price), float(trade.fee))
    elif trade.side == Side.SELL:
        stale_portfolio.sell(trade.symbol, trade.qty, float(trade.price), float(trade.fee))
    else:
        logger.warning(f"Unknown side for trade: {trade}")

    return _sorted_repair_portfolioo(stale_portfolio, target_portfolio, trades)


def repair_portfolio(
    stale_portfolio: Portfolio, target_portfolio: Portfolio, trades: List[Trade]
) -> Optional[Portfolio]:
    """
    Somtimes we might miss some trades.  Try to find and apply missing \
    trades to repair stale_portfolio to match positions in target_portfolio.
    This will repair the portoflio if we missed ALL of the last n trades for all n.
    i.e. it will not work if we missed some trades sporadically and applied trades at some later time.
    May need a smarter repair attempt, maybe keep all trade IDs in Portfolio states.
    """
    trades = sorted(trades, key=lambda t: cast(pd.Timestamp, t.exchange_ts), reverse=True)
    return _sorted_repair_portfolioo(stale_portfolio, target_portfolio, trades)
