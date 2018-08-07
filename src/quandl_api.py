import requests
from pathlib import Path
import pandas
import re
import os
import zipfile


class QuandlAPI:
    def __init__(self, api_key):
        self.api_key = api_key

    API_BASE = 'https://www.quandl.com/api/v3'

    DATA_FOLDER = 'data'
    COMPANIES_FILE = 'WIKI-datasets-codes.csv'
    PRICES_FOLDER = '{}/prices'.format(DATA_FOLDER)
    MARKET_FILE = 'NASDAQ-market-data.csv'

    def company_list_url(self) -> str:
        return '{}/databases/WIKI/{}/codes?api_key={}'.format(self.API_BASE, self.api_key)

    def market_url(self) -> str:
        return '{}/datasets/NASDAQOMX/COMP.csv?api_key={}'.format(self.API_BASE, self.api_key)

    def prices_url(self, ticker) -> str:
        return '{}/datasets/WIKI/{}.csv?api_key={}'.format(self.API_BASE, ticker, self.api_key)

    def download_companies(self):
        """
        Downloads a file containing a mapping of stock ticker -> company name
        :return: None
        """

        def _unzip(file, folder):
            zip_ref = zipfile.ZipFile(file, 'r')
            zip_ref.extractall('{}'.format(folder))
            zip_ref.close()

        companies_csv = '{}/{}'.format(self.DATA_FOLDER, self.COMPANIES_FILE)

        # If file already exists, don't re-download it.
        if os.path.isfile(companies_csv):
            return

        req = requests.get(self.company_list_url(), allow_redirects=True)
        companies_zip = '{}/companies.zip'.format(self.DATA_FOLDER)
        open(companies_zip, 'wb').write(req.content)
        _unzip(companies_zip, self.DATA_FOLDER)
        os.remove(companies_zip)

    def ticker_mapping(self):
        """
        Builds a mapping of stock ticker -> Company name
        :return: Dictionary of stock ticker string mapped to full name
        """

        def _desc_to_name(desc):
            name_search = re.search('(.*) \\((.*)\\) Prices.*', desc, re.IGNORECASE)
            if name_search:
                return name_search.group(1)
            return desc

        self.download_companies()
        companies_csv = '{}/{}'.format(self.DATA_FOLDER, self.COMPANIES_FILE)
        csv_file = Path(companies_csv)
        if not csv_file.is_file():
            print("Company CSV doesn't exist.")
            return {}
        csv = pandas.read_csv(csv_file,
                              header=None,
                              names=['Ticker', 'Description'])
        return {row['Ticker'][5:]: _desc_to_name(row['Description'])
                for row in csv.to_dict('records')}

    def market_prices(self):
        """
        :return: DataFrame with NASDAQ index prices
        """
        market_csv = '{}/{}'.format(self.DATA_FOLDER, self.MARKET_FILE)

        # Attempt to download market data, explicitly fail if we can't download
        if not os.path.isfile(market_csv):
            req = requests.get(self.market_url(), allow_redirects=True)
            if req.status_code != 200:
                print('Error encountered downloading market data.\nError: {}'.format(r.text))
                return None
            open(market_csv, 'wb').write(r.content)

        # Check if file exists, if it doesn't then fail
        csv_file = Path(market_csv)
        if not csv_file.is_file():
            print("Market Data CSV wasn't downloaded successfully")
            return None

        csv = pandas.read_csv(market_csv,
                              index_col='Trade Date',
                              parse_dates=['Trade Date']).sort_values(by=['Trade Date'])
        csv.index.names = ['Date']
        return csv

    def market_changes(self):
        """
        :return: DataFrame containing percent changes for NASDAQ index prices
        """
        data = self.market_prices()
        pct_changes = pandas.Series(data['Index Value'], index=data.index).pct_change()
        return pct_changes.fillna(0)

    def prices(self, ticker):
        """
        :param ticker: Stock ticker string
        :return: DataFrame containing adjusted closing prices for stock
        """
        url = self.prices_url(ticker)
        prices_csv = '{}/{}.csv'.format(self.PRICES_FOLDER, ticker)

        # Attempt to download price data, explicitly fail if we can't download
        if not os.path.isfile(prices_csv):
            req = requests.get(url, allow_redirects=True)
            if req.status_code != 200:
                print('Error encountered downloading {}.\n{}'.format(ticker, req.text))
                return None
            open(prices_csv, 'wb').write(req.content)

        # Check if file exists, if it doesn't then fail
        csv_file = Path(prices_csv)
        if not csv_file.is_file():
            print("Prices CSV for {} wasn't downloaded successfully".format(ticker))
            return None
        try:
            csv = pandas.read_csv(csv_file,
                                  index_col='Date',
                                  parse_dates=['Date']).sort_values(by=['Date'])
        except Exception as e:
            print("Couldn't parse {}.\n{}".format(ticker, e))
            return None

        return csv

    def price_changes(self, ticker):
        """
        :param ticker: Stock ticker string
        :return: DataFrame containing percent changes for a stock's adjusted closing prices
        """
        data = self.prices(ticker)
        if data is None:
            return None
        closes = pandas.Series(data['Adj. Close'], index=data.index)
        return closes.pct_change().fillna(0)
