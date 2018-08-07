import pandas
import os
from datetime import datetime
from .quandl_api import QuandlAPI


class BetaCalculator:
    DF_CHANGES_PATH = './df_changes.pkl.gz'
    MARKET_END_DATE = datetime(2018, 4, 1)

    def __init__(self, quandl_api: QuandlAPI):
        self.df_changes = None
        self.quandl_api = quandl_api
        self.max_window = 1

    def _cap(self, df, df_market):
        """
        Trims off the ends of a DataFrame, given dataframe based on the length of our market data
        :param df: Pandas DataFrame with stock information
        :param df_market: Pandas DataFrame with market information
        :return: DataFrame
        """
        start_date = pandas.to_datetime(df_market.index[0])
        end_date = self.MARKET_END_DATE
        return df[(df.index >= start_date) & (df.index < end_date)]

    def build_df(self, tickers, ignore_cache=False):
        """
        Builds a DataFrame containing daily stock percent changes
        :param tickers: List of string stock tickers
        :param ignore_cache: If true, will ignore the pickled dataframe from disk
        :return: None
        """
        if self.df_changes is not None:
            return

        if not ignore_cache and os.path.isfile(self.DF_CHANGES_PATH):
            df_changes = pandas.read_pickle(self.DF_CHANGES_PATH, compression='gzip')
            df_changes = self._cap(df_changes, df_changes['NASDAQ'])
            if df_changes is not None:
                self.df_changes = df_changes
                return

        df_changes = pandas.DataFrame()

        self.max_window = len(df_changes)
        i = 0
        for ticker in tickers:
            # Only take first 500 tickers
            if i > 500:
                break
            i += 1
            data = self.quandl_api.price_changes(ticker)
            if data is not None:
                df_changes[ticker] = data

        df_changes['NASDAQ'] = self.quandl_api.market_changes()
        df_changes = df_changes.fillna(0)
        df_changes.to_pickle(self.DF_CHANGES_PATH, compression='gzip')
        self.df_changes = self._cap(df_changes, df_changes['NASDAQ'])

    def betas(self, tickers, window=None):
        """
        Calculates betas for given stock tickers, over a specific window
        :param tickers: List of stock tickers to calculate betas for
        :param window: Size of the window to calculate betas over. Basically calculates
                       betas over Not including a window will calculate
                       for the full amount of data we have
        :return: Dataframe with columns as betas for a given stock
        """
        if self.df_changes is None:
            return None

        if not window:
            window = self.max_window

        market_prices = self.df_changes['NASDAQ']
        for ticker in tickers:
            if ticker not in self.df_changes:
                raise Exception("Ticker doesn't exist in dataset. \n" +
                                "See <a href='/avail'>/avail</a> for list of tickers")

        stock_prices = self.df_changes[tickers]
        cov = stock_prices.rolling(window=window).cov(market_prices, pairwise=True)
        var = market_prices.rolling(window=window).var()
        return cov.div(var, axis=0).dropna(0)
