import requests
import yfinance as yf
import pandas as pd
import re
import time
import pytz
import os
import schedule
import numpy as np

from bs4 import BeautifulSoup
from IPython.display import display
from tabulate import tabulate
from datetime import datetime, timedelta, date
from pathlib import Path
from functools import reduce

import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

# parameter, function name: lower case
# variable: Upper case


def check_save_lock(
    p_option="MAINTENANCE_STKVN > REF", p_action="SAVE", nb_seconds=3600, to_print=False
):
    p_result = True
    monitor_dir = "D:/python_r_DA/monitor/"
    file_name = "MANAGE_MONITOR_EXECUTION_SUMMARY.pkl"  # Converted from RDS to PKL

    if p_action == "SAVE":
        my_pc = get_pc_name()

        if os.path.exists(os.path.join(monitor_dir, file_name)):
            data_old = read_file(monitor_dir, file_name)

            if data_old["date"].max() == pd.Timestamp("today").normalize():
                list_option = data_old["code"].unique()

                if p_option in list_option:
                    data_old = data_old[
                        data_old["date"] == pd.Timestamp("today").normalize()
                    ]
                else:
                    new_row = pd.DataFrame(
                        [
                            [p_option, pd.Timestamp("today").normalize()]
                            + [None] * (data_old.shape[1] - 2)
                        ],
                        columns=data_old.columns,
                    )
                    data_old = pd.concat([data_old, new_row], ignore_index=True)
                    data_old = data_old[
                        data_old["date"] == pd.Timestamp("today").normalize()
                    ]
            else:
                data_old = pd.DataFrame(
                    {"code": [p_option], "date": [pd.Timestamp("today").normalize()]}
                )
        else:
            data_old = pd.DataFrame(
                {"code": [p_option], "date": [pd.Timestamp("today").normalize()]}
            )

        if my_pc not in data_old.columns:
            data_old[my_pc] = None

        subset_data = data_old[data_old["code"] == p_option]

        if pd.isna(subset_data[my_pc].iloc[0]) or (
            subset_data[my_pc].iloc[0] != datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ):
            subset_data[my_pc] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            data_old.update(subset_data)

        data_old = data_old.drop_duplicates(subset="code", keep="last").sort_values(
            by="code"
        )

        data_convert = data_old.copy()
        columns_to_convert = data_convert.columns.difference(["code", "date"])

        for col in columns_to_convert:
            data_convert[col] = data_convert[col].apply(
                lambda x: x if pd.notna(x) else None
            )
            data_convert[col] = pd.to_datetime(data_convert[col], errors="coerce")

        data_convert["updated"] = data_convert[columns_to_convert].max(axis=1)
        data_convert["updated"] = data_convert["updated"].dt.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        data_old = data_old.merge(
            data_convert[["code", "updated"]], on="code", how="left"
        )
        print(data_old)  # replace with display function

        # Save operations
        save_file(data_old, monitor_dir, file_name)
        # save_file(data_old, "S:/SHINY/REPORT/PC_FUNCTIONS/", file_name)

        # Export to text and fst (if fst package available in Python)
        data_old.to_csv(
            os.path.join(monitor_dir, file_name.replace(".pkl", ".txt")),
            sep="\t",
            index=False,
        )

    elif p_action == "COMPARE":
        my_pc = get_pc_name()

        if os.path.exists(os.path.join(monitor_dir, file_name)):
            data_old = read_file(monitor_dir, file_name)
            data_old = data_old[data_old["date"] == pd.Timestamp("today").normalize()]
        else:
            data_old = pd.DataFrame()

        data_one = (
            data_old[data_old["code"] == p_option]
            if not data_old.empty
            else pd.DataFrame()
        )

        if not data_one.empty:
            past_time = (
                pd.to_datetime(data_one["updated"].iloc[0])
                if not pd.isna(data_one["updated"].iloc[0])
                else datetime.now()
            )
            diff_time = (datetime.now() - past_time).total_seconds()
            print(f"{p_option} : {round(diff_time, 3)}")
            p_result = diff_time > nb_seconds

    if to_print:
        print(p_result)
    return p_result


def get_pc_name():
    return os.getenv("COMPUTERNAME").upper()


def read_file(monitor_dir, file_name):
    # Placeholder function for reading RDS file
    try:
        return pd.read_pickle(os.path.join(monitor_dir, file_name))
    except Exception as e:
        print(f"Error reading RDS file: {e}")
        return pd.DataFrame()


def save_file(data, directory, file_name):
    # Placeholder function for saving RDS file
    data.to_pickle(os.path.join(directory, file_name))


def last_trading_day():
    # Mock function to get last trading day, replace with actual logic
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def download_vnd_index_prices_by_code(p_codesource="VNINDEX", code_int="INDVNINDEX"):
    data_list = []
    to_continue = True
    k = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    while to_continue:
        # Construct the URL
        url = f"https://finfo-api.vndirect.com.vn/v4/vnmarket_prices?sort=date&q=code:{p_codesource}~date:gte:2000-07-29~date:lte:{date.today()}&size=100&page={k}"

        try:
            # Make the request and parse JSON
            response = requests.get(url, headers=headers)
            x = response.json()

            # Check if data is available
            if "data" in x and len(x["data"]) > 0:
                # Convert data to DataFrame
                df = pd.DataFrame(x["data"])
                df.columns
                # Clean and rename columns
                df = df.rename(columns=lambda col: col.strip().lower())

                # Select and transform columns as per requirements
                df = pd.DataFrame(
                    {
                        "source": "VND",
                        "codesource": p_codesource.upper(),
                        "ticker": p_codesource.upper(),
                        "date": pd.to_datetime(df["date"]).dt.date,
                        "open": pd.to_numeric(df["open"], errors="coerce"),
                        "high": pd.to_numeric(df["high"], errors="coerce"),
                        "low": pd.to_numeric(df["low"], errors="coerce"),
                        "close": pd.to_numeric(df["close"], errors="coerce"),
                        "change": pd.to_numeric(df["change"], errors="coerce"),
                        "rt": pd.to_numeric(df["pctchange"], errors="coerce"),
                        "volume": pd.to_numeric(df["accumulatedvol"], errors="coerce"),
                        "turnover": pd.to_numeric(
                            df["accumulatedval"], errors="coerce"
                        ),
                    }
                )

                data_list.append(df)
                k += 1  # Move to the next page
            else:
                to_continue = False  # Stop if there's no more data

        except Exception as e:
            print(f"Error retrieving or processing data: {e}")
            to_continue = False  # Stop in case of error

    # Combine all pages into a single DataFrame
    if data_list:
        data_all = pd.concat(data_list, ignore_index=True)
        data_all = data_all.sort_values(by="date").reset_index(drop=True)

        # Calculate additional columns
        data_all["reference"] = data_all["close"] - data_all["change"]
        data_all["rt"] = data_all["change"] / data_all["reference"]
        data_all["code"] = code_int.upper()

        SHOW_DATA(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


def EPOCH_TO_DATE(x_epoch=1651671000, convert_to="CHARACTER"):
    if convert_to == "CHARACTER":
        x_res = datetime.fromtimestamp(x_epoch).strftime("%Y-%m-%d %H:%M:%S")
    elif convert_to == "DATETIME":
        x_res = datetime.fromtimestamp(x_epoch)
    elif convert_to == "DATE":
        x_res = datetime.fromtimestamp(x_epoch).date()
    else:
        raise ValueError(
            "Invalid value for convert_to. Choose from 'CHARACTER', 'DATETIME', or 'DATE'."
        )

    return x_res


def download_ent_prices_by_code(
    type="index",
    codesource="VN30",
    code="INDVN30",
    NbMinutes=11 * 365 * 24 * 60,
    OffsetDays=0,
    pInterval="1",
):

    # Calculate End.time and Start.time in epoch time
    end_time = round(datetime.now().timestamp()) - (OffsetDays * 24 * 60 * 60 - 1)
    start_time = round(end_time - NbMinutes * 60)

    # Construct the URL
    p_url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/{type}?from={start_time}&to={end_time}&symbol={codesource}&resolution={pInterval}"
    print("Download URL:", p_url)

    # Request data from URL
    response = requests.get(p_url)

    # Print raw JSON data for inspection
    try:
        data = response.json()
        print(
            "Raw JSON data:", data
        )  # Inspect the structure here to ensure it's what we expect
    except ValueError as e:
        print(f"Failed to parse JSON data. Error: {e}")
        return pd.DataFrame()

    # Convert data to DataFrame if available
    if len(data) > 1:
        df = pd.DataFrame(data)
        df["timestamp_vn"] = pd.to_datetime(df["t"], unit="s") + timedelta(hours=7)
        df["open"] = df["o"] * 1000
        df["high"] = df["h"] * 1000
        df["low"] = df["l"] * 1000
        df["close"] = df["c"] * 1000
        df["volume"] = df["v"]

        # Additional calculations
        df["hhmmss"] = df["timestamp_vn"].dt.strftime("%H:%M")
        df["date"] = df["timestamp_vn"].dt.date
        df["source"] = "ENT"
        df["code"] = code
        df["codesource"] = codesource
        df["ticker"] = codesource
        df["type"] = "IND" if type == "index" else "STK"
        df["timestamp"] = df["timestamp_vn"].astype(str)

        # Placeholder values for ref1 mappings
        ref1 = {"iso2": "VN", "iso3": "VNM", "country": "VIETNAM", "continent": "ASIA"}

        for col, val in ref1.items():
            df[col] = val
        df = df.drop(columns=["o", "h", "l", "c", "v"])

        return df
    else:
        print("No data rows available")
        return pd.DataFrame()


# x = DOWNLOAD_CAF_PRICES_BY_CODE(p_codesource="VNM", code_int="")


def DOWNLOAD_YAH_PRICES_BY_CODE(symbol="IBM", period="max"):
    """
    Downloads and merges price, dividend, and shares data for a given symbol.

    Parameters:
    symbol (str): The stock ticker symbol.
    period (str): The time period for which data should be downloaded.
                  Options include "1mo", "3mo", "1y", or "max" (default is "max").

    Returns:
    DataFrame: A DataFrame containing price, dividend, and shares data merged by date.
    """
    # symbol = "IBM"
    ticker = yf.Ticker(symbol)

    # Download historical price data
    price_data = ticker.history(period=period)[
        ["Open", "High", "Low", "Close", "Volume"]
    ]
    price_data = price_data.reset_index()
    price_data = CLEAN_COLNAMES(price_data)
    price_data["date"] = pd.to_datetime(price_data["date"]).dt.date

    # Download dividends and shares data
    try:
        dividend_data = ticker.dividends
        dividend_data = dividend_data.reset_index()
        dividend_data = CLEAN_COLNAMES(dividend_data)
        dividend_data["date"] = pd.to_datetime(dividend_data["date"]).dt.date
    except pytz.exceptions.UnknownTimeZoneError as e:
        shares_data = pd.DataFrame(columns=["date", "dividends"])

    # shares_data = ticker.get_shares_full(start=start_date, end=end_date)
    # shares_data = pd.DataFrame(list(shares_data.items()), columns=["date", "sharesout"])
    try:
        # Lấy dữ liệu shares từ ticker
        shares_data = ticker.get_shares_full()
        # Convert shares data to DataFrame
        shares_data = pd.DataFrame(
            list(shares_data.items()), columns=["date", "sharesout"]
        )
        shares_data["date"] = pd.to_datetime(
            shares_data["date"], errors="coerce"
        ).dt.date

    except pytz.exceptions.UnknownTimeZoneError as e:
        shares_data = pd.DataFrame(columns=["date", "sharesout"])

    # shares_data["date"] = pd.to_datetime(shares_data["date"]).dt.date

    df_info = ticker.info
    final_data = pd.merge(price_data, dividend_data, on="date", how="left")
    final_data = pd.merge(final_data, shares_data, on="date", how="left")
    final_data["sharesout"] = final_data["sharesout"].ffill()

    # Add the symbol column
    final_data["codesource"] = symbol
    final_data["market_cap"] = df_info.get("marketCap")
    final_data["float"] = df_info.get("floatShares")
    final_data["cur"] = df_info.get("currency")
    final_data["rt"] = final_data["close"] / final_data["close"].shift(1) - 1
    final_data["change"] = final_data["close"] - final_data["close"].shift(1)

    final_data = final_data.drop_duplicates(subset=["codesource", "date"])

    final_data["rt"] = final_data.groupby("codesource")["rt"].transform(
        lambda x: x.fillna(0)
    )
    final_data["rt_1"] = 1 + final_data["rt"]
    final_data["cumul"] = final_data.groupby("codesource")["rt_1"].cumprod()
    final_data["cumul_rev"] = (
        final_data.groupby("codesource")["cumul"].transform("last")
        / final_data["cumul"]
    )
    final_data["capi"] = final_data["market_cap"] * final_data["cumul_rev"]
    final_data = final_data.drop(columns=["rt_1", "cumul", "cumul_rev"])
    SHOW_DATA(final_data)

    return final_data


# Usage example
# ticker = 'IBM'
# symbol_data = DOWNLOAD_YAH_PRICES_BY_CODE(symbol = ticker, period="max")
# symbol_data = DOWNLOAD_YAH_PRICES_BY_CODE("META", period="max")
def CACULATE_CAPI(df):
    df = df.drop_duplicates(subset=["codesource", "date"])
    df["rt"] = df.groupby("codesource")["rt"].transform(lambda x: x.fillna(0))
    df["rt_1"] = 1 + df["rt"]
    df["cumul"] = df.groupby("codesource")["rt_1"].cumprod()
    df["cumul_rev"] = df.groupby("codesource")["cumul"].transform("last") / df["cumul"]
    df["capi"] = df["market_cap"] * df["cumul_rev"]
    df = df.drop(columns=["rt_1", "cumul", "cumul_rev"])
    SHOW_DATA(df)
    return df


def CACULATE_RT_CHANGE(df):
    if "close_adj" in df.columns:
        df["rt"] = df["close_adj"] / df["close_adj"].shift(1) - 1
        df["change"] = df["close_adj"] - df["close_adj"].shift(1)
    else:
        df["rt"] = df["close"] / df["close"].shift(1) - 1
        df["change"] = df["close"] - df["close"].shift(1)


def SHOW_DATA(df):
    print(tabulate(df, headers="keys", tablefmt="pretty", showindex=False))


def CONVERT_NUMBER(value):
    """Helper function to convert strings like '1.2B' into a numeric value."""
    try:
        if "B" in value:
            return float(value.replace("B", "").replace(",", "")) * 1e9
        elif "M" in value:
            return float(value.replace("M", "").replace(",", "")) * 1e6
        elif "K" in value:
            return float(value.replace("K", "").replace(",", "")) * 1e3
        else:
            return float(value.replace(",", ""))
    except ValueError:
        return None


def CLEAN_COLNAMES(df):
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


def check_time_between(start, end):
    """Check if the current time is between `start` and `end`."""
    now = datetime.now().time()
    start_time = datetime.strptime(start, "%H:%M").time()
    end_time = datetime.strptime(end, "%H:%M").time()
    return start_time <= now <= end_time


def get_last_weekdays(days):
    """Get the last `days` weekdays."""
    today = datetime.today()
    last_dates = [today - timedelta(days=i) for i in range(days * 2)]
    return pd.to_datetime([d for d in last_dates if d.weekday() < 5][:days])


def save_history(file_path, new_data, subset=None):
    if not os.path.exists(file_path):
        history = pd.DataFrame()
    else:
        history = pd.read_pickle(file_path)

    history = pd.concat([new_data, history], ignore_index=True).drop_duplicates(
        subset=subset
    )
    history.to_pickle(file_path)

    print(f"History saved to {file_path}. Current record count: {len(history)}")


def SHOW_DATA(df):
    print(tabulate(df.head(5), headers="keys", tablefmt="pretty", showindex=False))


def DOWNLOAD_CAF_PRICES_BY_CODE(
    p_codesource="vnindex", code_int="INDVNINDEX", ToHistory=False
):
    data_list = []
    to_continue = True
    k = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    while to_continue:
        # Construct the URL
        url = f"https://s.cafef.vn/Ajax/PageNew/DataHistory/PriceHistory.ashx?Symbol={p_codesource}&StartDate=&EndDate=&PageIndex={k}&PageSize=100"

        try:
            # Make the request and parse JSON
            response = requests.get(url, headers=headers)
            x = response.json()

            # Check if data is available
            if "Data" in x and "Data" in x["Data"] and len(x["Data"]["Data"]) > 0:
                # Convert data to DataFrame
                df = pd.DataFrame(x["Data"]["Data"])

                # Clean and rename columns
                df = df.rename(columns=lambda col: col.strip().lower())
                df["thaydoi"] = df["thaydoi"].astype(str)
                df["khoiluongkhoplenh"] = df["khoiluongkhoplenh"].astype(str)
                df["giatrikhoplenh"] = df["giatrikhoplenh"].astype(str)
                # Select and transform columns as per requirements
                df = pd.DataFrame(
                    {
                        "source": "CAF",
                        "codesource": p_codesource.upper(),
                        "ticker": p_codesource.upper(),
                        "date": pd.to_datetime(
                            df["ngay"], format="%d/%m/%Y", errors="coerce"
                        ),
                        "open": pd.to_numeric(df["giamocua"], errors="coerce"),
                        "high": pd.to_numeric(df["giacaonhat"], errors="coerce"),
                        "low": pd.to_numeric(df["giathapnhat"], errors="coerce"),
                        "close_adj": pd.to_numeric(df["giadieuchinh"], errors="coerce"),
                        "close": pd.to_numeric(df["giadongcua"], errors="coerce"),
                        "change": pd.to_numeric(
                            df["thaydoi"].str.split("(", expand=True)[0],
                            errors="coerce",
                        ),
                        "varpc": pd.to_numeric(
                            df["thaydoi"].str.extract(r"\((.*?)%\)")[0], errors="coerce"
                        ),
                        "volume": pd.to_numeric(
                            df["khoiluongkhoplenh"].str.replace(",", ""),
                            errors="coerce",
                        ),
                        "turnover": pd.to_numeric(
                            df["giatrikhoplenh"].str.replace(",", ""), errors="coerce"
                        ),
                    }
                )

                data_list.append(df)
                k += 1  # Move to the next page
                if not ToHistory:
                    break
            else:
                to_continue = False  # Stop if there's no more data

        except Exception as e:
            print(f"Error retrieving or processing data: {e}")
            to_continue = False  # Stop in case of error

    # Combine all pages into a single DataFrame
    if data_list:
        data_all = pd.concat(data_list, ignore_index=True)
        data_all = data_all.sort_values(by="date").reset_index(drop=True)

        # Calculate additional columns
        data_all["reference"] = data_all["close"] - data_all["change"]
        data_all["rt"] = data_all["change"] / data_all["reference"]
        if code_int:
            data_all["code"] = code_int.upper()
        else:
            data_all["code"] = f"STKVN{p_codesource}"

        SHOW_DATA(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


def DOWNLOAD_CAF_FOREIGN_BY_CODE(
    p_codesource="vnindex", code_int="INDVNINDEX", ToHistory=False
):
    data_list = []
    to_continue = True
    k = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    while to_continue:
        # Construct the URL
        url = f"https://s.cafef.vn/Ajax/PageNew/DataHistory/GDKhoiNgoai.ashx?Symbol={p_codesource}&StartDate=&EndDate=&PageIndex={k}&PageSize=100"

        try:
            # Make the request and parse JSON
            response = requests.get(url, headers=headers)
            x = response.json()

            # Check if data is available
            if "Data" in x and "Data" in x["Data"] and len(x["Data"]["Data"]) > 0:
                # Convert data to DataFrame
                x_data = pd.DataFrame(x["Data"]["Data"])

                # Clean and rename columns
                x_data = x_data.rename(columns=lambda col: col.strip().lower())
                x_data = pd.DataFrame(
                    {
                        "source": "CAF",
                        "codesource": p_codesource.upper(),
                        "ticker": p_codesource.upper(),
                        "date": pd.to_datetime(
                            x_data["ngay"], format="%d/%m/%Y", errors="coerce"
                        ),
                        "buy_volume_foreign": pd.to_numeric(
                            x_data["klmua"], errors="coerce"
                        ),
                        "sell_volume_foreign": pd.to_numeric(
                            x_data["klban"], errors="coerce"
                        ),
                        "buy_turnover_foreign": pd.to_numeric(
                            x_data["gtmua"], errors="coerce"
                        ),
                        "sell_turnover_foreign": pd.to_numeric(
                            x_data["gtban"], errors="coerce"
                        ),
                    }
                )

                if code_int:
                    x_data["code"] = code_int

                data_list.append(x_data)
                k += 1  # Move to the next page
                if not ToHistory:
                    break
            else:
                to_continue = False  # Stop if there's no more data

        except Exception as e:
            print(f"Error retrieving or processing data: {e}")
            to_continue = False  # Stop in case of error

    # Combine all pages into a single DataFrame
    if data_list:
        data_all = pd.concat(data_list, ignore_index=True)
        data_all = data_all.sort_values(by="date").reset_index(drop=True)
        if code_int:
            data_all["code"] = code_int.upper()
        else:
            data_all["code"] = f"STKVN{p_codesource}"
        SHOW_DATA(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


def DOWNLOAD_CAF_ORDER_BY_CODE(
    p_codesource="vnindex", code_int="INDVNINDEX", ToHistory=False
):
    data_list = []
    to_continue = True
    k = 1

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    while to_continue:
        # Construct the URL
        url = f"https://s.cafef.vn/Ajax/PageNew/DataHistory/ThongKeDL.ashx?Symbol={p_codesource}&StartDate=&EndDate=&PageIndex={k}&PageSize=100"

        try:
            # Make the request and parse JSON
            response = requests.get(url, headers=headers)
            x = response.json()

            # Check if data is available
            if "Data" in x and "Data" in x["Data"] and len(x["Data"]["Data"]) > 0:
                # Convert data to DataFrame
                x_data = pd.DataFrame(x["Data"]["Data"])

                # Clean and rename columns
                x_data = x_data.rename(columns=lambda col: col.strip().lower())
                x_data = pd.DataFrame(
                    {
                        "source": "CAF",
                        "codesource": p_codesource.upper(),
                        "ticker": p_codesource.upper(),
                        "date": pd.to_datetime(
                            x_data["date"], format="%d/%m/%Y", errors="coerce"
                        ),
                        "buy_volume_order": pd.to_numeric(
                            x_data["kldatmua"], errors="coerce"
                        ),
                        "sell_volume_order": pd.to_numeric(
                            x_data["kldatban"], errors="coerce"
                        ),
                        "buy_order": pd.to_numeric(
                            x_data["solenhmua"], errors="coerce"
                        ),
                        "sell_order": pd.to_numeric(
                            x_data["solenhdatban"], errors="coerce"
                        ),
                    }
                )

                if code_int:
                    x_data["code"] = code_int

                data_list.append(x_data)
                k += 1  # Move to the next page
                if not ToHistory:
                    break
            else:
                to_continue = False  # Stop if there's no more data

        except Exception as e:
            print(f"Error retrieving or processing data: {e}")
            to_continue = False  # Stop in case of error

    # Combine all pages into a single DataFrame
    if data_list:
        data_all = pd.concat(data_list, ignore_index=True)
        data_all = data_all.sort_values(by="date").reset_index(drop=True)
        if code_int:
            data_all["code"] = code_int.upper()
        else:
            data_all["code"] = f"STKVN{p_codesource}"
        # SHOW_DATA(data_all)
        return data_all
    else:
        return pd.DataFrame()  # Return empty DataFrame if no data


def download_ent_prices_by_code(
    type="index",
    codesource="VN30",
    code="INDVN30",
    NbMinutes=11 * 365 * 24 * 60,
    OffsetDays=0,
    pInterval="1",
):

    # Calculate End.time and Start.time in epoch time
    end_time = round(datetime.now().timestamp()) - (OffsetDays * 24 * 60 * 60 - 1)
    start_time = round(end_time - NbMinutes * 60)

    # Construct the URL
    p_url = f"https://services.entrade.com.vn/chart-api/v2/ohlcs/{type}?from={start_time}&to={end_time}&symbol={codesource}&resolution={pInterval}"
    print("Download URL:", p_url)

    # Request data from URL
    response = requests.get(p_url)

    # Print raw JSON data for inspection
    try:
        data = response.json()
        print(
            "Raw JSON data:", data
        )  # Inspect the structure here to ensure it's what we expect
    except ValueError as e:
        print(f"Failed to parse JSON data. Error: {e}")
        return pd.DataFrame()

    # Convert data to DataFrame if available
    if len(data) > 1:
        df = pd.DataFrame(data)
        df["timestamp_vn"] = pd.to_datetime(df["t"], unit="s") + timedelta(hours=7)
        df["open"] = df["o"] * 1000
        df["high"] = df["h"] * 1000
        df["low"] = df["l"] * 1000
        df["close"] = df["c"] * 1000
        df["volume"] = df["v"]

        # Additional calculations
        df["hhmmss"] = df["timestamp_vn"].dt.strftime("%H:%M")
        df["date"] = df["timestamp_vn"].dt.date
        df["source"] = "ENT"
        df["code"] = code
        df["codesource"] = codesource
        df["ticker"] = codesource
        df["type"] = "IND" if type == "index" else "STK"
        df["timestamp"] = df["timestamp_vn"].astype(str)

        # Placeholder values for ref1 mappings
        ref1 = {"iso2": "VN", "iso3": "VNM", "country": "VIETNAM", "continent": "ASIA"}

        for col, val in ref1.items():
            df[col] = val
        df = df.drop(columns=["o", "h", "l", "c", "v"])

        return df
    else:
        print("No data rows available")
        return pd.DataFrame()


def download_imbalance_data_by_list(list_codes=None, do_history=False):
    """
    Downloads and processes imbalance data for a list of stock codes.

    Parameters:
        list_codes (list): A list of stock codes to process.

    Returns:
        pd.DataFrame: Final processed data with imbalance information.
    """
    # Split list into chunks of 100
    if len(list_codes) > 0:
        split_lists = [list_codes[i : i + 100] for i in range(0, len(list_codes), 100)]
        final_data = pd.DataFrame()

        for k, sublist in enumerate(split_lists, start=1):
            lock_file = Path(f"D:/python_r_DA/waiting_{k}.lock")

            # Check if lock file exists
            if lock_file.exists():
                lock_age = time.time() - lock_file.stat().st_mtime
                if lock_age > 600:  # Lock older than 10 minutes
                    print(f"Lock file exists too long, removing: {lock_file}")
                    lock_file.unlink()

            # Create lock file if it doesn't exist
            if not lock_file.exists():
                lock_file.touch()
                try:
                    x_list = []
                    # code = "FPT"
                    for code in sublist:
                        try:
                            # Fetch foreign data
                            foreign_data = DOWNLOAD_CAF_FOREIGN_BY_CODE(
                                p_codesource=code,
                                code_int="",
                                ToHistory=do_history,
                            )

                            # Fetch imbalance data based on time
                            # if check_time_between("09:15", "23:59"):
                            #     imbalance_data = download_funan_order_data_by_code(code)
                            # else:
                            imbalance_data = DOWNLOAD_CAF_ORDER_BY_CODE(
                                p_codesource=code,
                                code_int="",
                                ToHistory=do_history,
                            )

                            if not imbalance_data.empty:
                                # Merge foreign data
                                if not foreign_data.empty:
                                    imbalance_data = pd.merge(
                                        imbalance_data,
                                        foreign_data[
                                            [
                                                "code",
                                                "date",
                                                "buy_volume_foreign",
                                                "sell_volume_foreign",
                                                "buy_turnover_foreign",
                                                "sell_turnover_foreign",
                                            ]
                                        ],
                                        on=["code", "date"],
                                        how="left",
                                    )
                                    imbalance_data["date"] = pd.to_datetime(
                                        imbalance_data["date"]
                                    )
                                # Fetch price data
                                # price_data = download_ent_prices_by_code(
                                #     type="stock",
                                #     codesource=code,
                                #     code=f"STKVN{code}",
                                #     NbMinutes=11 * 365 * 24 * 60,
                                #     OffsetDays=0,
                                #     pInterval="1",
                                # )
                                SHOW_DATA(imbalance_data)

                                price_data = DOWNLOAD_CAF_PRICES_BY_CODE(
                                    p_codesource=code,
                                    code_int="",
                                    ToHistory=do_history,
                                )

                                SHOW_DATA(price_data)

                                if not price_data.empty and price_data.shape[0] > 5:
                                    # price_data = price_data.sort_values(
                                    #     "timestamp_vn", ascending=False
                                    # ).drop_duplicates(subset=["date"])
                                    price_data[["open", "low", "high", "close"]] *= 1000
                                    price_data["date"] = pd.to_datetime(
                                        price_data["date"]
                                    )

                                    imbalance_data = pd.merge(
                                        imbalance_data,
                                        price_data[["code", "date", "high", "low"]],
                                        on=["code", "date"],
                                        how="left",
                                    )

                                    # Calculate turnovers
                                    mid_price = (
                                        imbalance_data["high"] + imbalance_data["low"]
                                    ) / 2
                                    imbalance_data["buy_turnover"] = (
                                        mid_price * imbalance_data["buy_volume_order"]
                                    )
                                    imbalance_data["sell_turnover"] = (
                                        mid_price * imbalance_data["sell_volume_order"]
                                    )

                                    # Filter rows with valid data
                                    imbalance_data = imbalance_data.dropna(
                                        subset=["low", "high"]
                                    )

                                x_list.append(imbalance_data)
                        except Exception as e:
                            print(f"Error processing code {code}: {e}")

                    # Combine data and save
                    if x_list:
                        combined_data = pd.concat(x_list, ignore_index=True)
                        if not os.path.exists(
                            "D:/python_r_DA/data/dbl_download_data_imbalance_day.pkl"
                        ):
                            old_data = pd.DataFrame()
                        else:
                            old_data = pd.read_pickle(
                                "D:/python_r_DA/data/dbl_download_data_imbalance_day.pkl"
                            )

                        final_data = pd.concat(
                            [combined_data, old_data], ignore_index=True
                        ).drop_duplicates(subset=["code", "date"])

                        # SHOW_DATA(final_data)

                        # Filter for the last 10 weekdays
                        if not do_history:
                            last_weekdays = get_last_weekdays(10)
                            final_data = final_data[
                                final_data["date"] >= last_weekdays.min()
                            ]
                            final_data.to_pickle(
                                "D:/python_r_DA/data/dbl_download_data_imbalance_day.pkl"
                            )
                            SHOW_DATA(final_data)

                finally:
                    lock_file.unlink()

        ToMergeHistory = check_save_lock(
            p_option="MERGE_DATA_IMBALANCE_HISTORY",
            p_action="COMPARE",
            nb_seconds=3600,
            to_print=False,
        )

        if ToMergeHistory:
            if not os.path.exists(
                "D:/python_r_DA/data/dbl_download_data_imbalance_history.pkl"
            ):
                history = pd.DataFrame()
            else:
                history = pd.read_pickle(
                    "D:/python_r_DA/data/dbl_download_data_imbalance_history.pkl"
                )

            history = pd.concat(
                [final_data, history], ignore_index=True
            ).drop_duplicates(subset=["code", "date"])

            SHOW_DATA(history)

            history.to_pickle(
                "D:/python_r_DA/data/dbl_download_data_imbalance_history.pkl"
            )
            check_save_lock(
                p_option="MERGE_DATA_IMBALANCE_HISTORY",
                p_action="SAVE",
                nb_seconds=3600,
                to_print=False,
            )

        return final_data
    else:
        return pd.DataFrame()


# file_path = "D:/python_r_DA/list_file/list_stkvn.txt"
# with open(file_path, "r") as file:
#     # Skip the first line (column name)
#     next(file)

#     # Read the rest of the lines and strip any leading/trailing whitespace
#     codes_list = [line.strip() for line in file]

# dataa = download_imbalance_data_by_list(list_codes=codes_list, do_history=True)


def caculate_imbalance_index(ToHistory=False, ratio_accept=0.001, ToSave=True):
    if ToHistory:
        old_data = pd.read_pickle(
            "D:/python_r_DA/data/dbl_download_data_imbalance_history.pkl"
        )
    else:
        old_data = pd.read_pickle(
            "D:/python_r_DA/data/dbl_download_data_imbalance_day.pkl"
        )
    # Calculate imf
    old_data.loc[
        old_data["buy_volume_foreign"] + old_data["sell_volume_foreign"] != 0, "imf"
    ] = (
        (old_data["buy_volume_foreign"] - old_data["sell_volume_foreign"])
        / (old_data["buy_volume_foreign"] + old_data["sell_volume_foreign"])
        * 100
    )
    old_data.loc[
        old_data["buy_volume_foreign"] + old_data["sell_volume_foreign"] == 0, "imf"
    ] = np.nan

    # Calculate imf_turnover
    old_data.loc[
        old_data["buy_turnover_foreign"] + old_data["sell_turnover_foreign"] != 0,
        "imf_turnover",
    ] = (
        (old_data["buy_turnover_foreign"] - old_data["sell_turnover_foreign"])
        / (old_data["buy_turnover_foreign"] + old_data["sell_turnover_foreign"])
        * 100
    )
    old_data.loc[
        old_data["buy_turnover_foreign"] + old_data["sell_turnover_foreign"] == 0,
        "imf_turnover",
    ] = np.nan

    # Calculate imv
    old_data.loc[
        old_data["buy_volume_order"] + old_data["sell_volume_order"] != 0, "imv"
    ] = (
        (old_data["buy_volume_order"] - old_data["sell_volume_order"])
        / (old_data["buy_volume_order"] + old_data["sell_volume_order"])
        * 100
    )
    old_data.loc[
        old_data["buy_volume_order"] + old_data["sell_volume_order"] == 0, "imv"
    ] = np.nan

    # Calculate imo
    old_data.loc[old_data["buy_order"] + old_data["sell_order"] != 0, "imo"] = (
        (old_data["buy_order"] - old_data["sell_order"])
        / (old_data["buy_order"] + old_data["sell_order"])
        * 100
    )
    old_data.loc[old_data["buy_order"] + old_data["sell_order"] == 0, "imo"] = np.nan

    # Calculate imt
    old_data.loc[old_data["buy_turnover"] + old_data["sell_turnover"] != 0, "imt"] = (
        (old_data["buy_turnover"] - old_data["sell_turnover"])
        / (old_data["buy_turnover"] + old_data["sell_turnover"])
        * 100
    )
    old_data.loc[old_data["buy_turnover"] + old_data["sell_turnover"] == 0, "imt"] = (
        np.nan
    )

    old_data = old_data.dropna(subset=["imo", "imv"])

    old_data["code_und"] = old_data["code"]

    cols = ["imo", "imv", "imf", "imt", "imf_turnover"]
    old_data["imbalance_number"] = old_data[cols].fillna(0).sum(axis=1) / old_data[
        cols
    ].notna().sum(axis=1)

    old_data.loc[
        (old_data["imbalance_number"] >= -100) & (old_data["imbalance_number"] < -60),
        "imbalance_level",
    ] = "VERY FEAR"
    old_data.loc[
        (old_data["imbalance_number"] >= -60) & (old_data["imbalance_number"] < -20),
        "imbalance_level",
    ] = "FEAR"
    old_data.loc[
        (old_data["imbalance_number"] >= -20) & (old_data["imbalance_number"] < 20),
        "imbalance_level",
    ] = "NEUTRAL"
    old_data.loc[
        (old_data["imbalance_number"] >= 20) & (old_data["imbalance_number"] < 60),
        "imbalance_level",
    ] = "GREED"
    old_data.loc[
        (old_data["imbalance_number"] >= 60) & (old_data["imbalance_number"] <= 100),
        "imbalance_level",
    ] = "VERY GREED"

    old_data["code"] = "INDIMB" + old_data["code_und"]

    SHOW_DATA(old_data)

    stk_ref = pd.read_csv("D:/python_r_DA/list_file/dbl_stk_mother.txt")
    stk_ref["code_und"] = stk_ref["code"]
    old_data = pd.merge(
        old_data,
        stk_ref[["code_und", "name", "market", "industry", "sector"]],
        on=["code_und"],
        how="left",
    )

    old_data["name_und"] = old_data["name"]
    old_data["name"] = "BEQ IMBALANCE " + old_data["name_und"]

    if not os.path.exists("D:/python_r_DA/data/dbl_index_imbalance_day.pkl"):
        final_data = pd.DataFrame()
    else:
        final_data = pd.read_pickle("D:/python_r_DA/data/dbl_index_imbalance_day.pkl")

    final_data = pd.concat([final_data, old_data], ignore_index=True).drop_duplicates(
        subset=["code", "date"]
    )

    if not ToHistory:
        max_date = final_data["date"].max(skipna=True)
        final_data = final_data[final_data["date"] >= (max_date - pd.Timedelta(days=1))]

        final_data.to_pickle("D:/python_r_DA/data/dbl_index_imbalance_day.pkl")

        ToMergeHistory = check_save_lock(
            p_option="MERGE_INDEX_IMBALANCE_HISTORY",
            p_action="COMPARE",
            nb_seconds=3600,
            to_print=False,
        )

        if ToMergeHistory:
            if not os.path.exists(
                "D:/python_r_DA/data/dbl_index_imbalance_history.pkl"
            ):
                history = pd.DataFrame()
            else:
                history = pd.read_pickle(
                    "D:/python_r_DA/data/dbl_index_imbalance_history.pkl"
                )

            history = pd.concat(
                [final_data, history], ignore_index=True
            ).drop_duplicates(subset=["code", "date"])
            history.to_pickle("D:/python_r_DA/data/dbl_index_imbalance_history.pkl")
            check_save_lock(
                p_option="MERGE_INDEX_IMBALANCE_HISTORY",
                p_action="SAVE",
                nb_seconds=3600,
                to_print=False,
            )
    else:
        if not os.path.exists("D:/python_r_DA/data/dbl_index_imbalance_history.pkl"):
            final_data = pd.DataFrame()
        else:
            final_data = pd.read_pickle(
                "D:/python_r_DA/data/dbl_index_imbalance_history.pkl"
            )

        final_data = pd.concat(
            [final_data, old_data], ignore_index=True
        ).drop_duplicates(subset=["code", "date"])
        final_data.to_pickle("D:/python_r_DA/data/dbl_index_imbalance_history.pkl")

    return final_data
