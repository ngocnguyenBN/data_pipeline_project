import datetime
import enum
import json
import logging
import random
import re
import string
import pandas as pd
from websocket import create_connection
import requests
import json
import sys
import os

# from tvDatafeed import TvDatafeed, Interval
from dotenv import load_dotenv

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

dotenv_path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
load_dotenv(dotenv_path)

tdv_username = os.getenv("tdv_username")
tdv_password = os.getenv("tdv_password")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from .common_fn import *

logger = logging.getLogger(__name__)


def download_tdv_prices_by_code():
    tv = TvDatafeed()
    # index
    nifty_index_data = tv.get_hist(
        symbol="NIFTY", exchange="NSE", interval=Interval.in_1_hour, n_bars=1000
    )
    print(nifty_index_data)
    # futures continuous contract
    nifty_futures_data = tv.get_hist(
        symbol="NIFTY",
        exchange="NSE",
        interval=Interval.in_1_hour,
        n_bars=1000,
        fut_contract=1,
    )
    print(nifty_futures_data)

    # crudeoil
    crudeoil_data = tv.get_hist(
        symbol="CRUDEOIL",
        exchange="MCX",
        interval=Interval.in_1_hour,
        n_bars=5000,
        fut_contract=1,
    )
    print(crudeoil_data)

    # downloading data for extended market hours
    extended_price_data = tv.get_hist(
        symbol="EICHERMOT",
        exchange="NSE",
        interval=Interval.in_1_hour,
        n_bars=500,
        extended_session=False,
    )
    print(extended_price_data)


# For lazy loading web
def scroll_and_wait_for_element(driver, element_pos, by=By.CSS_SELECTOR, timeout=5):
    page_source = driver.find_element(By.XPATH, "//*").get_attribute("outerHTML")
    soup = BeautifulSoup(page_source, "html.parser")
    found = False
    if by == By.CSS_SELECTOR:
        element = soup.select_one(element_pos)
        if element:
            found = True
    elif by == By.XPATH:
        dom = etree.HTML(str(soup))
        links = dom.xpath(element_pos)
        if links:
            found = True

    if found:
        print(f"✅ Found element in page source: {element_pos}")
        if by == By.CSS_SELECTOR:
            scroll_script = f"""
            var element = document.querySelector("{element_pos}");
            if (element) element.scrollIntoView({{behavior: "instant", block: "center"}});
            """
        elif by == By.XPATH:
            scroll_script = f"""
            var element = document.evaluate("{element_pos}", document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue;
            if (element) element.scrollIntoView({{behavior: "instant", block: "center"}});
            """

        driver.execute_script(scroll_script)
        time.sleep(1)

        try:
            element = WebDriverWait(driver, timeout).until(
                EC.visibility_of_element_located((by, element_pos))
            )
            print(f"✅ Found & visible after direct scroll: {element_pos}")
            return element
        except TimeoutException:
            print(f"🚫 Found in source, scrolled, but not visible: {element_pos}")
            return None
    else:
        print(f"🔍 Not found in initial page source, start scrolling...")

    scroll_step = 500
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        try:
            element = WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((by, element_pos))
            )
            print(f"✅ Find element successfully: {element_pos}")
            return element
        except TimeoutException:
            pass

        driver.execute_script(f"window.scrollBy(0, {scroll_step});")
        time.sleep(1.5)

        new_height = driver.execute_script("return document.body.scrollHeight")
        current_position = driver.execute_script(
            "return window.scrollY + window.innerHeight"
        )

        if current_position >= new_height and new_height == last_height:
            print("🚫 Don't find any elements.")
            return None

        last_height = new_height


def scrape_all_tradingview_tabs():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-position=-2400,-2400")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("--no-proxy-server")
    chrome_options.add_argument("--force-device-scale-factor=0.9")

    # chrome_options.add_argument(
    #     "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    # )
    s = Service()
    driver = webdriver.Chrome(service=s, options=chrome_options)
    driver.set_window_size(1920, 1080)
    tab_texts = ["Popular", "Stocks& Indices", "Futures", "Forex", "Crypto"]
    # tab_texts = ["Popular", "Stocks& Indices", "Futures", "Forex", "Crypto", "Economy"]

    try:
        driver.get("https://www.tradingview.com/data-coverage/")
        exchange_data = []
        # Economy
        for tab_text in tab_texts:
            print(f"Processing: {tab_text}")

            if tab_text in [
                "Stocks& Indices",
                "Stocks & Indices",
                "Stocks &Indices",
                "Stocks&Indices",
            ]:
                tab_text = "Stocks\\&\\ Indices"
            tab_button = scroll_and_wait_for_element(
                driver, element_pos=f"#{tab_text}", by=By.CSS_SELECTOR, timeout=10
            )

            if tab_button:
                ActionChains(driver).move_to_element(tab_button).click().perform()

                while True:
                    try:
                        showmore_button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable(
                                (By.CLASS_NAME, "showLaptop-qJcpoITA")
                            )
                        )
                        showmore_button.click()
                    except TimeoutException:
                        break

                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "table-qJcpoITA"))
                )

                table_rows = driver.find_elements(
                    By.CSS_SELECTOR, "table.table-qJcpoITA tbody tr"
                )
                row_count = len(table_rows)
                print(
                    f"Number of rows in table after modifying tab {tab_text}: {row_count}"
                )

                exchange_elements = driver.find_elements(
                    By.CLASS_NAME, "rowWrap-qJcpoITA"
                )

                for element in exchange_elements:
                    exchange_name = ""
                    if len(
                        element.find_elements(
                            By.XPATH, ".//span[@class='exchangeName-qJcpoITA']"
                        )
                    ):
                        exchange_name = element.find_element(
                            By.XPATH, ".//span[@class='exchangeName-qJcpoITA']"
                        ).text

                    exchange_desc_name = ""

                    if len(
                        element.find_elements(
                            By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']"
                        )
                    ):
                        exchange_desc_name = element.find_element(
                            By.XPATH, ".//span[@class='exchangeDescName-qJcpoITA']"
                        ).text
                        pass

                    country = ""
                    if len(
                        element.find_elements(
                            By.XPATH,
                            ".//img[contains(@class, 'exchangeCountryFlag-qJcpoITA')]",
                        )
                    ):
                        country = element.find_element(
                            By.XPATH,
                            ".//img[contains(@class, 'exchangeCountryFlag-qJcpoITA')]",
                        ).get_attribute("alt")
                        pass

                    types = [
                        badge.text
                        for badge in element.find_elements(
                            By.XPATH, ".//span[@class='content-PlSmolIm']"
                        )
                    ]

                    exchange_data.append(
                        {
                            "tab": tab_text,
                            "exchangeName": exchange_name,
                            "exchangeDescName": exchange_desc_name,
                            "country": country,
                            "types": types,
                        }
                    )
                print("exchange_data", exchange_data)
            else:
                print("No Data")
        df = pd.DataFrame(exchange_data)

        return df
    except Exception as e:
        print(f"Failed: {e}")
        return None

    finally:
        driver.quit()


def download_all_indices_tradingview(quote=""):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-position=-2400,-2400")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("--no-proxy-server")
    chrome_options.add_argument("--force-device-scale-factor=0.9")

    # chrome_options.add_argument(
    #     "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    # )
    if quote:
        quote_list = [quote]
    else:
        quote_list = [
            "major",
            "snp",
            "us",
            "americas",
            "currency",
            "europe",
            "asia",
            "pacific",
            "middle-east",
            "africa",
        ]
    quote_mappings = {
        "major": "World Indices",
        "snp": "S&P 500",
        "us": "US Indices",
        "americas": "Americas Indices",
        "currency": "Currencies",
        "europe": "European Indices",
        "asia": "Asian Indices",
        "pacific": "Pacific Indices",
        "middle-east": "Middle East Indices",
        "africa": "African Indices",
    }
    all_data = []
    tradingview_path = (
        "/opt/airflow/FINANCEDATASCRAPER/database/scraping_raw_json/tradingview"
    )

    os.makedirs(tradingview_path, exist_ok=True)
    for quote in quote_list:
        s = Service()
        driver = webdriver.Chrome(service=s, options=chrome_options)
        driver.set_window_size(1920, 1080)
        try:
            driver.get(f"https://www.tradingview.com/markets/indices/quotes-{quote}/")
            table_rows = driver.find_elements(
                By.CSS_SELECTOR, "table.table-Ngq2xrcG tbody tr"
            )
            headers = driver.find_element(By.CLASS_NAME, "tableHead-RHkwFEqU")
            headers = headers.find_elements(By.TAG_NAME, "th")
            column_names = []
            for header in headers:
                header_text = header.text.strip()
                if not header_text:
                    header_text = header.get_attribute("data-field")
                if header_text:
                    column_names.append(header_text)

            rows = driver.find_elements(By.CLASS_NAME, "row-RdUXZpkv")
            row_keys = []
            cell_data_lists = []

            for row in rows:
                row_key = row.get_attribute("data-rowkey")

                if row_key:
                    row_keys.append(row_key)

                cells = row.find_elements(By.TAG_NAME, "td")
                cell_data = []
                for idx, cell in enumerate(cells):
                    if idx == 0:
                        symbol_text = ""
                        description_text = ""
                        try:
                            ticker_name_element = cell.find_element(
                                By.CSS_SELECTOR, "a.tickerName-GrtoTeat"
                            )
                            symbol_text = ticker_name_element.text.strip()
                        except:
                            pass
                        try:
                            description_element = cell.find_element(
                                By.CSS_SELECTOR, "sup.tickerDescription-GrtoTeat"
                            )
                            description_text = description_element.text.strip()
                        except:
                            pass
                        full_text = "\n".join(
                            filter(None, [symbol_text, description_text])
                        ).strip()
                    else:
                        full_text = cell.text.strip()
                    cell_data.append(full_text)
                if any(cell_data):
                    cell_data_lists.append(cell_data)

            df = pd.DataFrame(
                cell_data_lists, index=row_keys, columns=column_names
            ).reset_index()

            df.rename(columns={"index": "Exchange"}, inplace=True)
            split_df = df["Symbol"].str.split("\n", expand=True)

            if split_df.shape[1] < 2:
                split_df = split_df.reindex(columns=range(2), fill_value="")

            df[["Ticker", "Name"]] = split_df[[0, 1]].fillna("")
            df = df.drop(columns=["Symbol"])

            currency_columns = ["Price", "Change", "High", "Low"]
            for column in currency_columns:
                if column in df.columns:
                    df[[column, "Cur"]] = df[column].str.extract(
                        r"([\d.,]+)\s*([A-Z]{3})?", expand=True
                    )
                    df[column] = pd.to_numeric(
                        df[column].str.replace(",", ""), errors="coerce"
                    )
                    df["Cur"] = df["Cur"].replace("", pd.NA)
                    df["Cur"] = df["Cur"].astype(str).replace("nan", "")
            if "Change %" in df.columns:
                df["Change %"] = df["Change %"].str.replace("−", "-")
                df["Change %"] = (
                    pd.to_numeric(df["Change %"].str.replace("%", ""), errors="coerce")
                    / 100
                )
                df["Change %"] = df["Change %"].fillna(0)
                df.rename(columns={"Change %": "Rt"}, inplace=True)
            df["Tab"] = quote_mappings.get(quote, "Unknown")
            all_data.append(df)
        except Exception as e:
            print(f"Failed: {e}")
            df = pd.DataFrame()
        finally:
            driver.quit()
    final_data = pd.concat(all_data, ignore_index=True)
    final_data.to_json(
        f"{tradingview_path}/tradingview_all_indices.json", orient="records", indent=4
    )
    return "Scrape Prices Indices Sucessfully"
