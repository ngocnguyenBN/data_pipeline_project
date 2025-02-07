import yfinance as yf
import pandas as pd

# List of tickers
# tickers = ['AAPL', 'MSFT', 'GOOGL']import yfinance as yf
import pandas as pd

# Assuming 'list_code' is loaded from your R environment
# For the sake of the example, I'll simulate the data structure.

codes = [
    "0001.KL",
    "ZIGA.BK",
    "ZINC.JK",
    "ZKC.SI",
    "ZONE.JK",
    "ZYRX.JK",
]  # Replace with actual codes
file_path = "S:/R/DATA/list_country_company.txt"

# Open the file and read the content
with open(file_path, "r") as file:
    # Skip the first line (column name)
    next(file)

    # Read the rest of the lines and strip any leading/trailing whitespace
    codes_list = [line.strip() for line in file]

# Print the list to verify
print(codes_list)

# Initialize dictionaries to store data
historical_data = {}
sharesout_data = {}
dividends_data = {}

for code in codes_list:
    # Download historical data
    ticker = yf.Ticker(code)

    # Historical market data
    hist = ticker.history(period="max")
    historical_data[code] = hist

    # Shares outstanding (assuming it's available via 'get_shares_full')
    shares_out = ticker.get_shares_full()
    sharesout_data[code] = shares_out

    # Dividends
    dividends = ticker.dividends
    dividends_data[code] = dividends

# Save data to Excel files
with pd.ExcelWriter("D:/python_r_data_analysis/data/historical_data.xlsx") as writer:
    for code, data in historical_data.items():
        data.to_excel(writer, sheet_name=code)

with pd.ExcelWriter("D:/python_r_data_analysis/data/sharesout_data.xlsx") as writer:
    for code, data in sharesout_data.items():
        data.to_excel(writer, sheet_name=code)

with pd.ExcelWriter("D:/python_r_data_analysis/data/dividends_data.xlsx") as writer:
    for code, data in dividends_data.items():
        data.to_excel(writer, sheet_name=code)

print("Data download and saving completed.")


import pandas as pd


# Ensure that datetime columns are timezone-unaware
def remove_timezone(df):
    if isinstance(df, pd.DataFrame):
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)
    return df


# Combine all data into one DataFrame for each data type
def combine_data(data_dict):
    combined_df = pd.DataFrame()  # Initialize an empty DataFrame
    for code, data in data_dict.items():
        # Remove timezones if present
        data = remove_timezone(data)
        # Add a new column for the stock code
        data["Code"] = code
        # Append the data to the combined DataFrame
        combined_df = pd.concat([combined_df, data], axis=0)
    return combined_df


# Combine all historical data
combined_historical = combine_data(historical_data)

# Combine all shares outstanding data
combined_sharesout = combine_data(sharesout_data)

# Combine all dividends data
combined_dividends = combine_data(dividends_data)

# Save all combined data into one Excel file with separate sheets
with pd.ExcelWriter("D:/python_r_data_analysis/data/combined_data.xlsx") as writer:
    combined_historical.to_excel(writer, sheet_name="PRICES", index=False)
    combined_sharesout.to_excel(writer, sheet_name="SHARES", index=False)
    combined_dividends.to_excel(writer, sheet_name="DIVIDENDS", index=False)

print("All combined data has been saved into a single Excel file with separate sheets.")


import yfinance as yf

msft = yf.Ticker("MSFT")
msft.history(period="max")


# Specify the file path
file_path = "S:/R/DATA/list_composition_SP500.txt"
file_path = "S:/R/DATA/list_country_company.txt"

file_path = "D:/list_Code_USA.txt"
# Open the file and read the content
with open(file_path, "r") as file:
    # Skip the first line (column name)
    next(file)

    # Read the rest of the lines and strip any leading/trailing whitespace
    codes_list = [line.strip() for line in file]

# Print the list to verify
print(codes_list)

# Initialize an empty list to collect the data
data = []

for ticker in codes_list:
    # Fetch ticker information
    t = yf.Ticker(ticker)
    info = t.info

    # Add ticker code to the info dictionary
    info["Code"] = ticker

    # Append the info dictionary to the data list
    data.append(info)

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(data)

# Reorder columns if needed, e.g., placing 'Code' as the first column
columns = ["Code"] + [col for col in df.columns if col != "Code"]
df = df[columns]

print(df)

# Save the DataFrame to an Excel file
output_file = "D:/python_r_data_analysis/data/ticker_info_new.xlsx"
df.to_excel(output_file, index=False, engine="openpyxl")

print(f"Data saved to {output_file}")
# # get all stock info
# msft.info


import yfinance as yf
import pandas as pd

# Specify the file path
file_path = "S:/R/DATA/list_composition_SP500.txt"

# Open the file and read the content
with open(file_path, "r") as file:
    # Skip the first line (column name)
    next(file)

    # Read the rest of the lines and strip any leading/trailing whitespace
    codes_list = [line.strip() for line in file]

# Print the list to verify
print(codes_list)

# Create a dictionary to store share data for each ticker
shares_data = []

for ticker in codes_list:
    # Fetch full share data (assuming get_shares_full is a method you have)
    t = yf.Ticker(ticker)
    shares_full = t.get_shares_full().reset_index()

    # shares_df = pd.DataFrame(shares_full)
    # Rename columns to 'date' and 'shareout'
    shares_full.rename(columns={"index": "date", 0: "shareout"}, inplace=True)

    # Remove timezone information from 'date' column
    if pd.api.types.is_datetime64tz_dtype(shares_full["date"]):
        shares_full["date"] = shares_full["date"].dt.tz_convert(None)

    # Add the ticker code to the DataFrame
    shares_full["code"] = ticker
    # Store the share data in the dictionary
    # shares_data[ticker] = shares_full
    shares_data.append(shares_full)

df = pd.concat(shares_data, ignore_index=True)
df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.tz_localize(None)

df.dtypes
output_file_shares = "D:/ticker_shares_full_combined.xlsx"
df.to_excel(output_file_shares, index=False, engine="openpyxl")


import yfinance as yf
import pandas as pd


# Helper function to remove timezones from datetime columns
def remove_timezone(df):
    if isinstance(df, pd.DataFrame):
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.tz_localize(None)
    return df


# List of codes (your codes_list)
# codes_list = ["AAPL", "GOOGL", "MSFT"]  # Example, replace with actual codes


# Function to retrieve historical, sharesout, and dividends data
def fetch_data_for_code(code):
    ticker = yf.Ticker(code)

    # Historical data (check if valid data is returned)
    try:
        historical = ticker.history(period="max")
        if historical is not None and not historical.empty:
            historical["Code"] = code  # Add stock code column
        else:
            historical = pd.DataFrame()  # Empty DataFrame as fallback
    except Exception as e:
        print(f"Error fetching historical data for {code}: {e}")
        historical = pd.DataFrame()  # Empty DataFrame as fallback

    # Shares outstanding (check if valid data is returned)
    # try:
    #     sharesout = ticker.get_shares_full()
    #     if sharesout is not None and not sharesout.empty:
    #         sharesout['Code'] = code  # Add stock code column
    #     else:
    #         sharesout = pd.DataFrame()  # Empty DataFrame as fallback
    # except Exception as e:
    #     print(f"Error fetching sharesout data for {code}: {e}")
    #     sharesout = pd.DataFrame()  # Empty DataFrame as fallback

    # Dividends (check if valid data is returned)
    try:
        dividends = ticker.dividends
        if dividends is not None and not dividends.empty:
            dividends = dividends.to_frame(name="Dividends")
            dividends["Code"] = code  # Add stock code column
        else:
            dividends = pd.DataFrame()  # Empty DataFrame as fallback
    except Exception as e:
        print(f"Error fetching dividends data for {code}: {e}")
        dividends = pd.DataFrame()  # Empty DataFrame as fallback

    return {
        "historical": remove_timezone(historical),
        # 'sharesout': sharesout,
        "dividends": remove_timezone(dividends),
    }


# Fetch data for all stock codes using list comprehension
data_dict = {code: fetch_data_for_code(code) for code in codes_list}

# Combine the data for all codes into one DataFrame for each type
combined_historical = pd.concat(
    [data["historical"] for data in data_dict.values() if not data["historical"].empty]
)
combined_sharesout = pd.concat(
    [data["sharesout"] for data in data_dict.values() if not data["sharesout"].empty]
)
combined_dividends = pd.concat(
    [data["dividends"] for data in data_dict.values() if not data["dividends"].empty]
)

# Save the combined data into one Excel file with different sheets
with pd.ExcelWriter("D:/python_r_data_analysis/data/combined_data.xlsx") as writer:
    combined_historical.to_excel(writer, sheet_name="HistoricalData", index=False)
    combined_sharesout.to_excel(writer, sheet_name="SharesoutData", index=False)
    combined_dividends.to_excel(writer, sheet_name="DividendsData", index=False)

print("All combined data has been saved into one Excel file.")

output_file_price = "D:/python_r_data_analysis/data/prices.xlsx"
output_file_shares = "D:/python_r_data_analysis/data/shares.xlsx"

output_file_dividend = "D:/python_r_data_analysis/data/dividend.xlsx"

combined_dividends.to_excel(output_file_dividend, index=False, engine="openpyxl")


combined_historical["date"] = pd.to_datetime(
    combined_historical["Date"], errors="coerce"
).dt.tz_localize(None)

a = combined_historical.reset_index()
a.to_csv("D:/python_r_data_analysis/data/prices.txt", sep="\t", index=False)


import yfinance as yf
import pandas as pd


file_path = "D:/OneDrive/WCEO_RESEARCH.xlsx"
data = pd.read_excel(file_path, sheet_name="DETAIL", engine="openpyxl")
codes_list = data["ticker"].tolist()
# Print the list to verify
print(codes_list)

# Initialize an empty list to collect the data
data = []

for ticker in codes_list:
    # Fetch ticker information
    t = yf.Ticker(ticker)
    info = t.info

    # Extract only the market cap, currency, and add the ticker code
    data.append(
        {
            "codesource": ticker,
            "capi": info.get("marketCap"),
            "cur": info.get("currency"),
        }
    )

# Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(data)

# Print the resulting DataFrame to verify
print(df)


# # get historical market data
# hist = msft.history(period="1mo")

# # show meta information about the history (requires history() to be called first)
# msft.history_metadata

# # show actions (dividends, splits, capital gains)
# msft.actions
# msft.dividends
# msft.splits
# msft.capital_gains  # only for mutual funds & etfs

# # show share count
# msft.get_shares_full(start="2022-01-01", end=None)

# # show financials:
# msft.calendar
# msft.sec_filings
# # - income statement
# msft.income_stmt
# msft.quarterly_income_stmt
# # - balance sheet
# msft.balance_sheet
# msft.quarterly_balance_sheet
# # - cash flow statement
# msft.cashflow
# msft.quarterly_cashflow
# # see `Ticker.get_income_stmt()` for more options

# # show holders
# msft.major_holders
# msft.institutional_holders
# msft.mutualfund_holders
# msft.insider_transactions
# msft.insider_purchases
# msft.insider_roster_holders

# msfs.sustainability

# # show recommendations
# msft.recommendations
# msft.recommendations_summary
# msft.upgrades_downgrades

# # show analysts data
# msft.analyst_price_targets
# msft.earnings_estimate
# msft.revenue_estimate
# msft.earnings_history
# msft.eps_trend
# msft.eps_revisions
# msft.growth_estimates

# # Show future and historic earnings dates, returns at most next 4 quarters and last 8 quarters by default.
# # Note: If more are needed use msft.get_earnings_dates(limit=XX) with increased limit argument.
# msft.earnings_dates

# # show ISIN code - *experimental*
# # ISIN = International Securities Identification Number
# msft.isin

# # show options expirations
# msft.options

# # show news
# msft.news

# # get option chain for specific expiration
# opt = msft.option_chain('YYYY-MM-DD')
# # data available via: opt.calls, opt.puts

import pandas as pd
import json
import re


def safe_parse_json(json_text):
    if pd.isna(json_text) or json_text == "":
        return None  # Return None for empty or invalid JSON

    try:
        # Replace problematic escape sequences (e.g., \x96) with a valid character
        json_text_clean = re.sub(r"\\x[0-9A-Fa-f]{2}", "-", json_text)

        # Replace single quotes used as JSON delimiters while preserving apostrophes
        json_text_fixed = re.sub(r"(?<!\")'|'(?!\")", '"', json_text_clean)

        # Fix keys with single quotes
        json_text_fixed = re.sub(
            r"([{,])\s*\'([^\']+)\'\s*:", r'\1"\2":', json_text_fixed
        )

        # Fix values with single quotes
        json_text_fixed = re.sub(
            r":\s*\'([^\']+)\'\s*([,}])", r': "\1"\2', json_text_fixed
        )

        # Handle remaining single quotes (general case)
        json_text_fixed = re.sub(
            r"(?<![a-zA-Z])'([^']*)'(?![a-zA-Z])", r'"\1"', json_text_fixed
        )

        # Parse the corrected JSON string
        parsed_json = json.loads(json_text_fixed)

        return parsed_json
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return None


# Example usage
# Load your Excel file
data_info = pd.read_excel("D:/python_r_data_analysis/data/ticker_info_new.xlsx")

# Apply the function to the JSON column
data_info["parsed_json"] = data_info["companyOfficers"].apply(safe_parse_json)

# Flatten the JSON structure into individual columns
expanded_json = pd.json_normalize(data_info["parsed_json"])

# Combine the expanded JSON with the original data
data_info_expanded = pd.concat(
    [data_info.drop(columns=["companyOfficers", "parsed_json"]), expanded_json], axis=1
)

# View the final result
print(data_info_expanded["Code" == "5288.T"])
