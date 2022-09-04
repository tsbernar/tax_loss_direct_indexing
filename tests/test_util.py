import pandas as pd

from tax_loss.util import Schedule


def test_schedule():
    json_data = [
        {
            "clearingCycleEndTime": "0000",
            "tradingScheduleDate": "20000101",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0000", "closingTime": "0000", "cancelDayOrders": "N"}],
        },
        {
            "clearingCycleEndTime": "0000",
            "tradingScheduleDate": "20000102",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0000", "closingTime": "0000", "cancelDayOrders": "N"}],
        },
        {
            "clearingCycleEndTime": "2000",
            "tradingScheduleDate": "20000103",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}],
        },
        {
            "clearingCycleEndTime": "2000",
            "tradingScheduleDate": "20000104",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}],
        },
        {
            "clearingCycleEndTime": "2000",
            "tradingScheduleDate": "20000105",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}],
        },
        {
            "clearingCycleEndTime": "2000",
            "tradingScheduleDate": "20000106",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}],
        },
        {
            "clearingCycleEndTime": "2000",
            "tradingScheduleDate": "20000107",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0930", "closingTime": "1600", "prop": "LIQUID", "cancelDayOrders": "Y"}],
        },
        {
            "clearingCycleEndTime": "0000",
            "tradingScheduleDate": "20220905",
            "sessions": [],
            "tradingtimes": [{"openingTime": "0000", "closingTime": "0000", "cancelDayOrders": "N"}],
        },
    ]

    schedule = Schedule(json_data=json_data, exchange_tz="America/New_York")
    ts = pd.Timestamp("20220906 9:00").tz_localize("America/Chicago")  # regular day, regular time
    assert schedule.is_open(ts)
    ts = pd.Timestamp("20220906 17:00").tz_localize("America/Chicago")  # regular day, after market hours
    assert not schedule.is_open(ts)
    ts = pd.Timestamp("20220905 10:00").tz_localize("America/Chicago")  # holiday, regular time
    assert not schedule.is_open(ts)
