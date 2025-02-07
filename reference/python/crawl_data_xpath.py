import requests
from bs4 import BeautifulSoup
import yfinance as yf
import pandas as pd

from python.LIBRARY.python_library import DOWNLOAD_YAH_PRICES_BY_CODE


def trainee_download_yah_shares(pCode="IBM"):
    # Initialize final data
    final_data = pd.DataFrame()

    # Step 1: Scrape Yahoo Finance for key statistics
    pURL = f"https://au.finance.yahoo.com/quote/{pCode}/key-statistics"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    }

    response = requests.get(pURL, headers=headers)
    soup = BeautifulSoup(response.content, "html.parser")

    # Step 2: Extract float and shares outstanding values using XPath or HTML structure
    try:
        float_value = soup.select_one(
            "#Col1-0-KeyStatistics-Proxy section div:nth-of-type(2) div:nth-of-type(2) div div:nth-of-type(2) div div table tbody tr:nth-of-type(5) td:nth-of-type(2)"
        ).text.strip()
        float_value = convert_number(float_value)
    except:
        float_value = None

    try:
        sharesout_value = soup.select_one(
            "#Col1-0-KeyStatistics-Proxy section div:nth-of-type(2) div:nth-of-type(2) div div:nth-of-type(2) div div table tbody tr:nth-of-type(3) td:nth-of-type(2)"
        ).text.strip()
        sharesout_value = convert_number(sharesout_value)
    except:
        sharesout_value = None

    # Step 3: Get market capitalization using yfinance
    stock = yf.Ticker(pCode)
    market_cap = stock.info.get("marketCap", None)

    # If yfinance didn't retrieve marketCap, fall back to scraping the page for it
    if market_cap is None:
        pURL = f"https://au.finance.yahoo.com/quote/{pCode}/"
        response = requests.get(pURL, headers=headers)
        soup = BeautifulSoup(response.content, "html.parser")

        try:
            market_cap_value = soup.select_one(
                "#quote-summary div:nth-of-type(2) table tbody tr:nth-of-type(1) td:nth-of-type(2)"
            ).text.strip()
            market_cap = convert_number(market_cap_value)
        except:
            market_cap = None

    # Step 4: Create the final dataframe
    final_data = pd.DataFrame(
        {
            "codesource": [pCode],
            "share_outstanding": [sharesout_value],
            "float": [float_value],
            "market_cap": [market_cap],
        }
    )

    # Display the final data (You could use print or logging)
    print(final_data)

    return final_data


import pandas as pd


file_path = "D:/OneDrive/WCEO_RESEARCH.xlsx"
data = pd.read_excel(file_path, sheet_name="DETAIL", engine="openpyxl")

data["ticker"] = data["ticker"].str.strip()
data["country"] = data["country"].str.strip()

data["ticker"] = data["ticker"].fillna("")  # Thay thế NaN bằng chuỗi rỗng
data["ticker"] = data["ticker"].astype(str)

data["ticker"] = data["ticker"].apply(lambda x: "".join(e for e in x if e.isalnum()))

# codes_list = data["ticker"].tolist()
# prices = {}
# for code in codes_list:
#     prices[code] = DOWNLOAD_YAH_PRICES_BY_CODE(symbol=code, period="max")


# Xác nhận cột 'ticker' và 'country' tồn tại
if "ticker" in data.columns and "country" in data.columns:
    # Nhóm các ticker theo country
    grouped_data = data.groupby("country")

    # Duyệt qua từng nhóm (từng country)
    for country, group in grouped_data:
        prices = {}

        for ticker in group["ticker"]:
            # Gọi hàm tải dữ liệu
            prices[ticker] = DOWNLOAD_YAH_PRICES_BY_CODE(symbol=ticker, period="max")

        # Tạo DataFrame từ dictionary và lưu file
        country_prices_df = pd.DataFrame(prices)
        save_path = f"{country}_prices.xlsx"
        country_prices_df.to_excel(save_path, index=False)
        # print(f"Đã lưu dữ liệu cho {country} vào tệp {save_path}")
