import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from .common_fn import *
from .base import DataSource


def download_data_etfdb(pCodesource="IBIT", pNbdays=30):
    # Construct URL
    purl = (
        f"https://etfflows.websol.barchart.com/proxies/timeseries/queryeod.ashx?"
        f"symbol={pCodesource}&data=daily&maxrecords={pNbdays}&volume=contract&order=asc"
        f"&dividends=false&backadjust=false&daystoexpiration=1&contractroll=expiration"
    )

    # Send request
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "Accept-Language": "en-US,en;q=0.5",
    }
    response = requests.get(purl, headers=headers)

    # Check response
    if response.status_code != 200:
        print("Error fetching data")
        return pd.DataFrame()

    response_text = response.text

    # Filter lines containing the desired code
    lines = [
        line for line in response_text.split("\n") if line.startswith(f"{pCodesource},")
    ]

    if not lines:
        print("No data found")
        return pd.DataFrame()

    # Convert to DataFrame
    data = pd.read_csv(io.StringIO("\n".join(lines)), header=None)
    data.columns = ["codesource", "date", "open", "high", "low", "close", "volume"]
    # Add source column
    data["source"] = "ETFDB"
    print(data)
    return data


def download_yah_prices_intraday_by_code(
    pCodesource="^VIX", pInterval="5m", pNbdays=60, Hour_adjust=0
):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }
    my_url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{pCodesource}"
        f"?region=US&lang=en-US&includePrePost=false&interval={pInterval}"
        f"&range={pNbdays}d&corsDomain=finance.yahoo.com&.tsrc=finance"
    )

    response = requests.get(my_url, headers=headers)
    dt_json = response.json()

    df_combined = pd.DataFrame()

    if dt_json and "chart" in dt_json and dt_json["chart"]["result"]:
        result = dt_json["chart"]["result"][0]
        meta = result.get("meta", {})

        gmtoffset = meta.get("gmtoffset", 0)
        calc_hour_adjust = gmtoffset / 3600
        if calc_hour_adjust > 0:
            Hour_adjust = calc_hour_adjust

        rCodesource = meta.get("symbol", pCodesource)
        print(f"=== {rCodesource} ===")

        timestamps = result.get("timestamp", [])
        df_timestamp = pd.DataFrame({"timestamp": timestamps})
        df_timestamp["ID"] = range(1, len(df_timestamp) + 1)

        indicators = result.get("indicators", {})
        quote = indicators.get("quote", [{}])[0]

        if len(quote.get("open", [])) > 2:
            df_open = pd.DataFrame({"open": quote.get("open", [])})
            df_open["ID"] = range(1, len(df_open) + 1)

            df_high = pd.DataFrame({"high": quote.get("high", [])})
            df_high["ID"] = range(1, len(df_high) + 1)

            df_low = pd.DataFrame({"low": quote.get("low", [])})
            df_low["ID"] = range(1, len(df_low) + 1)

            df_close = pd.DataFrame({"close": quote.get("close", [])})
            df_close["ID"] = range(1, len(df_close) + 1)

            if "adjclose" in indicators and indicators["adjclose"]:
                df_closeadj = pd.DataFrame(
                    {
                        "close_adj": indicators["adjclose"][0].get(
                            "adjclose", quote.get("close", [])
                        )
                    }
                )
            else:
                df_closeadj = pd.DataFrame({"close_adj": quote.get("close", [])})
            df_closeadj["ID"] = range(1, len(df_closeadj) + 1)

            df_volume = pd.DataFrame({"volume": quote.get("volume", [])})
            df_volume["ID"] = range(1, len(df_volume) + 1)

            df_combined = df_timestamp.merge(df_open[["ID", "open"]], on="ID")
            df_combined = df_combined.merge(df_high[["ID", "high"]], on="ID")
            df_combined = df_combined.merge(df_low[["ID", "low"]], on="ID")
            df_combined = df_combined.merge(df_close[["ID", "close"]], on="ID")
            df_combined = df_combined.merge(df_closeadj[["ID", "close_adj"]], on="ID")
            df_combined = df_combined.merge(df_volume[["ID", "volume"]], on="ID")

            df_combined["datetime"] = pd.to_datetime(
                df_combined["timestamp"], unit="s", utc=True
            )
            df_combined["datetime"] = df_combined["datetime"] + pd.to_timedelta(
                Hour_adjust, unit="h"
            )

            cols_order = [
                "datetime",
                "open",
                "high",
                "low",
                "close",
                "close_adj",
                "volume",
            ]
            df_combined = df_combined[cols_order]

            df_combined["codesource"] = rCodesource
            df_combined["source"] = "YAH"
            cols_order = ["codesource"] + [
                col for col in df_combined.columns if col != "codesource"
            ]
            df_combined = df_combined[cols_order]

            df_combined["date"] = df_combined["datetime"].dt.date

            df_combined["timestamp"] = df_combined["datetime"]

            if pInterval == "1d":
                df_combined = df_combined.sort_values("timestamp").drop_duplicates(
                    subset="date", keep="first"
                )

    df_combined = update_updated(df_combined)
    view_data(df_combined)
    return df_combined


def download_yah_prices_by_code(ticker="AAPL", period="max"):
    try:
        # symbol = yf.Ticker(ticker)
        # price_data = symbol.history(period=period, auto_adjust=False)[
        #     ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends"]
        # ]
        price_data = yf.download(ticker, period=period, auto_adjust=False)
        price_data.columns = price_data.columns.droplevel(1)
        price_data = price_data.reset_index()
        price_data["ticker"] = ticker
        price_data = clean_colnames(price_data)
        price_data = update_updated(price_data)
        view_data(price_data)
        return price_data
    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}")
        return None


def download_yah_info_by_code(ticker="AAPL"):
    # symbol = yf.Ticker('MSFT')
    symbol = yf.Ticker(ticker)
    info_data = symbol.info
    info_data = clean_colnames(info_data)

    return info_data


def download_yah_financial_by_code(ticker="AAPL"):
    symbol = yf.Ticker(ticker)

    balance_sheet = symbol.balance_sheet.reset_index()
    balance_sheet["dataset"] = "BALANCE_SHEET"

    cash_flow = symbol.cash_flow.reset_index()
    cash_flow["dataset"] = "CASH_FLOW"

    income_stat = symbol.income_stmt.reset_index()
    income_stat["dataset"] = "INCOME_STATEMENT"

    financial_data = pd.concat(
        [balance_sheet, cash_flow, income_stat], ignore_index=True
    )
    financial_data = clean_colnames(financial_data)

    return financial_data


def download_yah_shares_by_code(ticker="AAPL"):
    symbol = yf.Ticker(ticker)
    shares_data = symbol.get_shares_full()
    shares_data = pd.DataFrame(list(shares_data.items()), columns=["date", "sharesout"])
    shares_data = self.clean_colnames(shares_data)

    return shares_data


# FOR STOCK
def download_yah_shares_html_by_code(ticker="AAPL"):
    shares_data = pd.DataFrame()
    headers = {
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        # "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    pURL = f"https://finance.yahoo.com/quote/{ticker}/key-statistics/"

    response = requests.get(pURL, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    try:
        float_value = soup.select_one(
            "#nimbus-app > section > section > section > article > article > div > section:nth-child(2) > div > section:nth-child(2) > table > tbody > tr:nth-child(5) > td.value.yf-vaowmx"
        ).text.strip()
        float_value = convert_number(float_value)
    except:
        float_value = None

    try:
        sharesout_value = soup.select_one(
            "#nimbus-app > section > section > section > article > article > div > section:nth-child(2) > div > section:nth-child(2) > table > tbody > tr:nth-child(3) > td.value.yf-vaowmx"
        ).text.strip()
        sharesout_value = convert_number(sharesout_value)
    except:
        sharesout_value = None

    shares_data = pd.DataFrame({"sharesout": [sharesout_value], "float": [float_value]})

    return shares_data


def download_yah_marketcap_html_by_code(ticker="AAPL"):

    pURL = f"https://finance.yahoo.com/quote/{ticker}/"
    headers = {
        # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
        # "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    response = requests.get(pURL, headers=headers)
    print(response)
    soup = BeautifulSoup(response.content, "html.parser")
    try:
        marketcap_value = soup.select_one(
            "#nimbus-app > section > section > section > article > div.container.yf-gn3zu3 > ul > li:nth-child(9) > span.value.yf-gn3zu3 > fin-streamer"
        ).text.strip()
        marketcap_value = convert_number(marketcap_value)
    except:
        marketcap_value = None

    return marketcap_value


# not connect db
# class YAHDataSource(DataSource):
#     def __init__(self, ticker="AAPL"):
#         self.ticker = ticker
#         self.headers = {
#             # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
#             "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
#             # "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
#             "Accept-Language": "en-US,en;q=0.9",
#         }
#         pass

#     def connect(self):
#         pass

#     def pull_data(self):
#         pass

#     def clean_data(self, raw_data):
#         pass

#     def close_connection(self):
#         pass

#     def process_data(self):
#         pass

#     def clean_colnames(self, df):
#         """
#         Cleans column names of a DataFrame by:
#         - Converting to lowercase
#         - Replacing spaces and hyphens with underscores
#         - Replacing '%' with 'percent_'
#         - Removing '*' characters
#         - Stripping any leading/trailing whitespace

#         Parameters:
#         df (pd.DataFrame): The DataFrame with columns to clean.

#         Returns:
#         pd.DataFrame: A new DataFrame with cleaned column names.
#         """
#         df = df.copy()  # Create a copy to avoid modifying the original DataFrame
#         df.columns = [
#             re.sub(
#                 r"\s+",
#                 "",  # Remove all whitespace
#                 re.sub(
#                     r"\-",
#                     "_",  # Replace hyphens with underscores
#                     re.sub(
#                         r"\%",
#                         "percent_",  # Replace '%' with 'percent_'
#                         re.sub(r"\*", "", col.lower()),
#                     ),
#                 ),
#             )  # Remove '*' and make lowercase
#             for col in df.columns
#         ]
#         return df

#     def get_data_ohlc(self, period="max"):
#         symbol = yf.Ticker(self.ticker)
#         price_data = symbol.history(period=period, auto_adjust=False)[
#             ["Open", "High", "Low", "Close", "Adj Close", "Volume", "Dividends"]
#         ]
#         # price_data = yf.download(self.ticker, start="2021-09-01", end="2025-02-10")

#         price_data = price_data.reset_index()
#         price_data = self.clean_colnames(price_data)
#         return price_data

#     def get_data_info(self):
#         # symbol = yf.Ticker('MSFT')
#         symbol = yf.Ticker(self.ticker)
#         info_data = symbol.info
#         info_data = self.clean_colnames(info_data)

#         return info_data

#     def get_data_financial(self):
#         symbol = yf.Ticker(self.ticker)

#         balance_sheet = symbol.balance_sheet.reset_index()
#         balance_sheet["dataset"] = "BALANCE_SHEET"

#         cash_flow = symbol.cash_flow.reset_index()
#         cash_flow["dataset"] = "CASH_FLOW"

#         income_stat = symbol.income_stmt.reset_index()
#         income_stat["dataset"] = "INCOME_STATEMENT"

#         financial_data = pd.concat(
#             [balance_sheet, cash_flow, income_stat], ignore_index=True
#         )
#         financial_data = self.clean_colnames(financial_data)

#         return financial_data

#     def get_data_sharesout(self):
#         symbol = yf.Ticker(self.ticker)
#         shares_data = symbol.get_shares_full()
#         shares_data = pd.DataFrame(
#             list(shares_data.items()), columns=["date", "sharesout"]
#         )
#         shares_data = self.clean_colnames(shares_data)

#         return shares_data

#     def convert_number(self, value):
#         """Helper function to convert strings like '1.2B' into a numeric value."""
#         try:
#             if "B" in value:
#                 return float(value.replace("B", "").replace(",", "")) * 1e9
#             elif "M" in value:
#                 return float(value.replace("M", "").replace(",", "")) * 1e6
#             elif "K" in value:
#                 return float(value.replace("K", "").replace(",", "")) * 1e3
#             elif "T" in value:
#                 return float(value.replace("T", "").replace(",", "")) * 1e12
#             else:
#                 return float(value.replace(",", ""))
#         except ValueError:
#             return None

#     # FOR STOCK
#     def get_data_shares_by_request(self):
#         shares_data = pd.DataFrame()

#         pURL = f"https://finance.yahoo.com/quote/{self.ticker}/key-statistics/"

#         response = requests.get(pURL, headers=self.headers)
#         soup = BeautifulSoup(response.content, "html.parser")

#         try:
#             float_value = soup.select_one(
#                 "#nimbus-app > section > section > section > article > article > div > section:nth-child(2) > div > section:nth-child(2) > table > tbody > tr:nth-child(5) > td.value.yf-vaowmx"
#             ).text.strip()
#             float_value = self.convert_number(float_value)
#         except:
#             float_value = None

#         try:
#             sharesout_value = soup.select_one(
#                 "#nimbus-app > section > section > section > article > article > div > section:nth-child(2) > div > section:nth-child(2) > table > tbody > tr:nth-child(3) > td.value.yf-vaowmx"
#             ).text.strip()
#             sharesout_value = self.convert_number(sharesout_value)
#         except:
#             sharesout_value = None

#         shares_data = pd.DataFrame(
#             {"sharesout": [sharesout_value], "float": [float_value]}
#         )

#         return shares_data

#     def get_value_marketcap_by_request(self):

#         pURL = f"https://finance.yahoo.com/quote/{self.ticker}/"
#         response = requests.get(pURL, headers=self.headers)
#         print(response)
#         soup = BeautifulSoup(response.content, "html.parser")
#         try:
#             marketcap_value = soup.select_one(
#                 "#nimbus-app > section > section > section > article > div.container.yf-gn3zu3 > ul > li:nth-child(9) > span.value.yf-gn3zu3 > fin-streamer"
#             ).text.strip()
#             marketcap_value = self.convert_number(marketcap_value)
#         except:
#             marketcap_value = None

#         return marketcap_value

# def save_data_to_postgre(self):
