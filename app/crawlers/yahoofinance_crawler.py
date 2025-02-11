from .base import DataSource
import requests
import yfinance as yf
import pandas as pd
import re
import time
import pytz
import schedule
import numpy as np

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from pathlib import Path
from functools import reduce
import os
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)


# not connect db
class YAHDataSource(DataSource):
    def __init__(self, ticker="AAPL"):
        self.ticker = ticker
        self.headers = {
            # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
            # "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
        pass

    def connect(self):
        pass

    def pull_data(self):
        pass

    def clean_data(self, raw_data):
        pass

    def close_connection(self):
        pass

    def process_data(self):
        pass

    def clean_colnames(self, df):
        """
        Cleans column names of a DataFrame by:
        - Converting to lowercase
        - Replacing spaces and hyphens with underscores
        - Replacing '%' with 'percent_'
        - Removing '*' characters
        - Stripping any leading/trailing whitespace

        Parameters:
        df (pd.DataFrame): The DataFrame with columns to clean.

        Returns:
        pd.DataFrame: A new DataFrame with cleaned column names.
        """
        df = df.copy()  # Create a copy to avoid modifying the original DataFrame
        df.columns = [
            re.sub(
                r"\s+",
                "",  # Remove all whitespace
                re.sub(
                    r"\-",
                    "_",  # Replace hyphens with underscores
                    re.sub(
                        r"\%",
                        "percent_",  # Replace '%' with 'percent_'
                        re.sub(r"\*", "", col.lower()),
                    ),
                ),
            )  # Remove '*' and make lowercase
            for col in df.columns
        ]
        return df

    def get_data_ohlc(self, period="max"):
        symbol = yf.Ticker(self.ticker)
        price_data = symbol.history(period=period, auto_adjust=False)[
            ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends"]
        ]
        # price_data = yf.download(self.ticker, start="2021-09-01", end="2025-02-10")

        price_data = price_data.reset_index()
        price_data = self.clean_colnames(price_data)
        return price_data

    def get_data_info(self):
        # symbol = yf.Ticker('MSFT')
        symbol = yf.Ticker(self.ticker)
        info_data = symbol.info
        info_data = self.clean_colnames(info_data)

        return info_data

    def get_data_financial(self):
        symbol = yf.Ticker(self.ticker)

        balance_sheet = symbol.balance_sheet.reset_index()
        balance_sheet["dataset"] = "BALANCE_SHEET"

        cash_flow = symbol.cash_flow.reset_index()
        cash_flow["dataset"] = "CASH_FLOW"

        income_stat = symbol.income_stmt.reset_index()
        income_stat["dataset"] = "INCOME_STATEMENT"

        financial_data = pd.concat(
            [balance_sheet, cash_flow, income_stat], ignore_index=True
        )
        financial_data = self.clean_colnames(financial_data)

        return financial_data

    def get_data_sharesout(self):
        symbol = yf.Ticker(self.ticker)
        shares_data = symbol.get_shares_full()
        shares_data = pd.DataFrame(
            list(shares_data.items()), columns=["date", "sharesout"]
        )
        shares_data = self.clean_colnames(shares_data)

        return shares_data

    def convert_number(self, value):
        """Helper function to convert strings like '1.2B' into a numeric value."""
        try:
            if "B" in value:
                return float(value.replace("B", "").replace(",", "")) * 1e9
            elif "M" in value:
                return float(value.replace("M", "").replace(",", "")) * 1e6
            elif "K" in value:
                return float(value.replace("K", "").replace(",", "")) * 1e3
            elif "T" in value:
                return float(value.replace("T", "").replace(",", "")) * 1e12
            else:
                return float(value.replace(",", ""))
        except ValueError:
            return None

    # FOR STOCK
    def get_data_shares_by_request(self):
        shares_data = pd.DataFrame()

        pURL = f"https://finance.yahoo.com/quote/{self.ticker}/key-statistics/"

        response = requests.get(pURL, headers=self.headers)
        soup = BeautifulSoup(response.content, "html.parser")

        try:
            float_value = soup.select_one(
                "#nimbus-app > section > section > section > article > article > div > section:nth-child(2) > div > section:nth-child(2) > table > tbody > tr:nth-child(5) > td.value.yf-vaowmx"
            ).text.strip()
            float_value = self.convert_number(float_value)
        except:
            float_value = None

        try:
            sharesout_value = soup.select_one(
                "#nimbus-app > section > section > section > article > article > div > section:nth-child(2) > div > section:nth-child(2) > table > tbody > tr:nth-child(3) > td.value.yf-vaowmx"
            ).text.strip()
            sharesout_value = self.convert_number(sharesout_value)
        except:
            sharesout_value = None

        shares_data = pd.DataFrame(
            {"sharesout": [sharesout_value], "float": [float_value]}
        )

        return shares_data

    def get_value_marketcap_by_request(self):

        pURL = f"https://finance.yahoo.com/quote/{self.ticker}/"
        response = requests.get(pURL, headers=self.headers)
        print(response)
        soup = BeautifulSoup(response.content, "html.parser")
        try:
            marketcap_value = soup.select_one(
                "#nimbus-app > section > section > section > article > div.container.yf-gn3zu3 > ul > li:nth-child(9) > span.value.yf-gn3zu3 > fin-streamer"
            ).text.strip()
            marketcap_value = self.convert_number(marketcap_value)
        except:
            marketcap_value = None

        return marketcap_value

    # def save_data_to_postgre(self):
        