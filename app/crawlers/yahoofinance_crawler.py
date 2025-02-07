from .base import DataSource
import requests
import yfinance as yf
import pandas as pd
import re
import time
import pytz
import schedule
import numpy as np

from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
from pathlib import Path
from functools import reduce
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)


# not connect db
class YAHDataSource(DataSource):
    def __init__(self, ticker="AAPL"):
        self.ticker = ticker
        self.spark = SparkSession.builder.appName("YahooFinanceCrawler").getOrCreate()
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
    
    def get_data_ohlc(self, period="max"):
        symbol = yf.Ticker(self.ticker)
        price_data = symbol.history(period=period)[
            ["Open", "High", "Low", "Close", "Volume", "Dividends"]
        ]
        price_data = price_data.reset_index()
        return self.spark.createDataFrame(price_data)

    def get_data_info(self):
        # symbol = yf.Ticker('MSFT')
        symbol = yf.Ticker(self.ticker)
        info_data = symbol.info
        return info_data

    def get_data_financial(self):
        symbol = yf.Ticker(self.ticker)
        balance_sheet = symbol.balance_sheet.reset_index()
        balance_sheet["dataset"] = "BALANCE_SHEET"
        cash_flow = symbol.cash_flow.reset_index()
        cash_flow["dataset"] = "CASH_FLOW"
        income_stat = symbol.income_stmt.reset_index()
        income_stat["dataset"] = "INCOME_STATMENT"
        # Convert Pandas DataFrames to Spark DataFrames
        balance_sheet_spark = self.spark.createDataFrame(balance_sheet)
        cash_flow_spark = self.spark.createDataFrame(cash_flow)
        income_stat_spark = self.spark.createDataFrame(income_stat)

        # Concatenate DataFrames using unionByName()
        financial_data = balance_sheet_spark.unionByName(cash_flow_spark).unionByName(
            income_stat_spark
        )
        financial_data = financial_data.T

        return self.spark.createDataFrame(financial_data.reset_index())
