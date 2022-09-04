import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd

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
