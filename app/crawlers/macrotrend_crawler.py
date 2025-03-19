from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import time
from selenium.common.exceptions import TimeoutException
from lxml import etree
from bs4 import BeautifulSoup
import os
from selenium.webdriver.chrome.service import Service
import random
from selenium.webdriver.common.keys import Keys
import polars as pl
from datetime import timedelta, datetime
import re
import requests
import json


def download_marcrotrend_marketcap_history_by_code(pCode="AAPL"):

    url = f"https://www.macrotrends.net/assets/php/market_cap.php?t={pCode}"
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    df = pl.DataFrame()
    if response.status_code == 200:
        html_content = response.text

        match = re.search(r"var chartData = (\[.*?\]);", html_content, re.S)

        if match:
            chart_data_json = match.group(1)
            chart_data = json.loads(chart_data_json)

            df = pl.DataFrame(chart_data)
            df = df.rename({"v1": "market_cap"})
            df = df.with_columns(
                (df["market_cap"] * 1000000000).alias("market_cap")
            )  # Billions
            df = df.with_columns(pl.lit(pCode).alias("codesource"))
        else:
            print("chartData not found.")
    else:
        print("Failed to fetch the webpage.")

    return df


def download_marcrotrend_stock_screener_by_code():

    url = "https://www.macrotrends.net/stocks/stock-screener"
    df = pl.DataFrame()

    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        html_text = response.text

        match = re.search(r"var originalData = (\[.*?\]);", html_text, re.DOTALL)

        if match:
            json_data = json.loads(match.group(1))

            def normalize_data(record):
                for key, value in record.items():
                    if isinstance(value, str):
                        value = value.strip()  # Xóa khoảng trắng dư
                        if value.lower() in ("null", "none", ""):
                            record[key] = None  # Chuyển giá trị rỗng thành None
                        else:
                            try:
                                if "." in value:  # Nếu có dấu chấm, chuyển thành float
                                    record[key] = float(value)
                                else:  # Nếu không, kiểm tra số nguyên
                                    record[key] = int(value)
                            except ValueError:
                                pass  # Nếu lỗi, giữ nguyên
                return record

            json_data = [normalize_data(record) for record in json_data]

            df = pl.DataFrame(
                json_data, schema_overrides=None, infer_schema_length=1000
            )

            print(df)
        else:
            print("No Data.")
    else:
        print("No Response.")

    return df
