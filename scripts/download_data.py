import datetime
import json
from io import StringIO
from time import sleep
import os

import click
import pandas as pd
import requests
from typing import Optional
import yfinance as yf

DEFAULT_START = "2022-07-01"  # first date available for IVV weights is 2006-09-29
DEFAULT_DATA_DIR = "./data"
IVV_WEIGHTS_FILE_NAME = "IVV_weights.parquet"
IVV_REQUESTED_DATES = "requested_dates.json"
IVV_WEIGHTS_BASE_URL = "https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?fileType=csv&fileName=IVV_holdings&dataType=fund&asOfDate="  # noqa: E501
TICKER_DATA_FILE_NAME = "yf_tickers.parquet"
EXTRA_YF_TICKERS = ["IVV"]

TICKER_CORRECTION_MAP = {
    "BRKB": "BRK-B",
    "GEC": "GE",
    "GE,": "GE",
    "GOOGL": "GOOG",
    "FB": "META",
    "ANTM": "ELV",  # name change on Jun 28, 2022
}


def clean_weight_df(df: pd.DataFrame, ticker_correction) -> pd.DataFrame:
    # clean up types
    df["Weight (%)"] = df["Weight (%)"].apply(lambda x: float(x.replace(",", "")) if type(x) is str else x)

    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")

    # only show equities and return subed tickers
    if "Asset Class" in df.columns:
        df = df[(df["Asset Class"] == "Equity") | (df["Asset Class"].isna())]

    if ticker_correction:
        df.Ticker = df.Ticker.replace(ticker_correction)

    # some tickers contian a * for when they were held in Bolsa Mexicana De Valores exchange instead of a US exchange
    # (Ex: AAPL*).  We can consider these equivilant to AAPL
    df.Ticker = df.Ticker.str.replace("*", "", regex=False)

    # someitmes there are duplicate entries with the same ticker (ex ones we have transformed), should add the weights
    df = df.groupby(["date", "Ticker"])["Weight (%)"].sum().reset_index()

    return df


def save_sp500_weighting_data(start_date, end_date, data_directory, ticker_correction=None):
    base_url = IVV_WEIGHTS_BASE_URL
    date_cache_file = data_directory + "/" + IVV_REQUESTED_DATES
    ivv_weight_file = data_directory + "/" + IVV_WEIGHTS_FILE_NAME

    #  build list of dates in ishares url format (yyyymmdd) starting with most recent.
    all_dates = pd.date_range(start_date, end_date)
    all_dates = list(pd.Series(all_dates)[::-1].apply(lambda x: x.strftime("%Y%m%d")))

    if os.path.exists(date_cache_file):
        requested_dates = json.load(open(date_cache_file))
    else:
        requested_dates = {}

    dates = [d for d in all_dates if (d not in requested_dates) or (requested_dates[d] is False)]
    print(f"skipping {len(all_dates) - len(dates)} already in cache, requesting {len(dates)} {dates}")

    if os.path.exists(ivv_weight_file):
        df = pd.read_parquet(ivv_weight_file)
    else:
        df = pd.DataFrame()

    # download data.. this will take a while if not cached
    csv_data = []
    for date in dates:
        if date in requested_dates and requested_dates[date]:
            continue
        url = base_url + date
        data = requests.get(url)
        if data.status_code != 200:
            print(f"failed dowload on {date} : {data.reason}")
            requested_dates[date] = False
            continue
        requested_dates[date] = True

        if len(df):
            df = df[df.date != date]  # Drop existing data if already exists

        csv = data.text[data.text.find("Ticker") :]
        csv = csv[: csv.find("\n\xa0\n")]

        if csv:
            csv = pd.read_csv(StringIO(csv))
            csv["date"] = data.text.split('Fund Holdings as of,"')[1].split('"')[0]
            csv_data.append(csv)

        # sleep for 50ms to not spam the api
        sleep(0.05)

    if len(csv_data):
        df = pd.concat([df, pd.concat(csv_data)]).drop_duplicates()

    df = clean_weight_df(df, ticker_correction)

    df.to_parquet(ivv_weight_file, compression="GZIP")
    with open(date_cache_file, "w") as f:
        f.write(json.dumps(requested_dates, sort_keys=True))


def concat_or_update(df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    rows_to_add = new_df.index.difference(df.index)
    df = pd.concat([df, new_df.loc[rows_to_add]])  # also adds missing cols even if 0 rows added
    df.update(new_df)
    df = df.sort_index()
    df = df.sort_index(axis=1)
    return df


def save_yf_data(start_date, end_date, tickers, filepath):
    filename = filepath + "/" + TICKER_DATA_FILE_NAME

    if os.path.exists(filename):
        df = pd.read_parquet(filename)
    else:
        df = pd.DataFrame()

    price_matrix = yf.download(tickers, start=start_date, end=end_date)

    price_matrix = price_matrix["Adj Close"]
    price_matrix = pd.DataFrame(price_matrix)  # Coerce to a df (will be Series if only 1 ticker)
    price_matrix.index = pd.to_datetime(price_matrix.index)

    min_days_valid_data = min(50, int(len(price_matrix) / 2))
    price_matrix = price_matrix.dropna(axis=1, thresh=min_days_valid_data).sort_index()

    df = concat_or_update(df, price_matrix)

    df.to_parquet(filename, compression="GZIP")

    return price_matrix


@click.command()
@click.option("--filepath", default=DEFAULT_DATA_DIR, type=str, help="Path to data directory")
@click.option("--start_date", default=DEFAULT_START, type=str)
@click.option("--end_date", type=str)
@click.option("--ticker_data", is_flag=True)
@click.option("--weight_data", is_flag=True)
def main(
    filepath: str,
    start_date: str,
    end_date: Optional[str],
    ticker_data: bool,
    weight_data: bool,
) -> None:
    if not end_date:
        end_date = datetime.date.today()

    if weight_data:
        print(f"Downloading IVV weight data from {start_date} to {end_date}")
        save_sp500_weighting_data(start_date, end_date, filepath, TICKER_CORRECTION_MAP)

    if ticker_data:
        print(f"Downloading ticker data from {start_date} to {end_date}")
        tickers = EXTRA_YF_TICKERS
        ivv_weights_file = filepath + "/" + IVV_WEIGHTS_FILE_NAME
        if os.path.exists(ivv_weights_file):
            df = pd.read_parquet(ivv_weights_file)
            tickers += list(df.Ticker.unique())
        print(f"Downloading ticker data for {len(tickers)} tickers")
        save_yf_data(start_date, end_date, tickers, filepath)


if __name__ == "__main__":
    main()
