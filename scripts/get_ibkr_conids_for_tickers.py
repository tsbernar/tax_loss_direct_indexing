import requests
import click
import pandas as pd
import urllib.parse
from typing import List

TICKER_DATA_FILE_NAME = "yf_tickers.parquet"  # TODO move these to central constants file
DEFAULT_DATA_DIR = "./data"
BASE_URL = 'https://localhost:5000/v1/api/trsrv/stocks/?symbols='

YF_TO_IBKR_TICKER_CORRECTION_MAP = {
    "BRK-B": "BRK B",
}


def make_url(tickers: List[str]) -> str:
    url = BASE_URL + ",".join(tickers)
    return urllib.parse.quote(url)


def process_response(tickers: List[str], r : requests.Response, filepath: str) -> None:
    data = r.json()

    ticker_data_list = []
    for ticker in tickers:
        if ticker not in data or not len(data[ticker]):
            print(f"{ticker} not found in IBKR response!")
            continue
        ticker_data = data[ticker][0]  # Asusume first result is right.. :/
        ticker_data["ticker"] = ticker
        ticker_data['conid'] = ticker_data['contracts'][0]['conid']
        ticker_data_list.append(ticker_data)

    df = pd.DataFrame(ticker_data_list)
    print(df)
    df.to_parquet(filepath + '/' + 'IBKR_conids')


@click.command()
@click.option("--filepath", default=DEFAULT_DATA_DIR, type=str, help="Path to data directory")
def main(filepath: str) -> None:
    df = pd.read_parquet(filepath + '/' + TICKER_DATA_FILE_NAME)
    tickers : List[str] = list(df.columns)
    tickers = [t if t not in YF_TO_IBKR_TICKER_CORRECTION_MAP else YF_TO_IBKR_TICKER_CORRECTION_MAP[t] for t in tickers]
    url = make_url(tickers)
    r = requests.post(url, verify=False)
    if r.status_code != 200:
        print(f'Error {r}: {r.text}')
        exit()
    process_response(tickers, r, filepath)


if __name__ == "__main__":
    main()
