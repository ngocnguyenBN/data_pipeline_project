import yfinance as yf
import pandas as pd

file_path = "S:/LIST/ETF.txt"
with open(file_path, "r") as file:
    # Skip the first line (column name)
    next(file)

    # Read the rest of the lines and strip any leading/trailing whitespace
    codes_list = [line.strip() for line in file]


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
combined_dividends = pd.concat(
    [data["dividends"] for data in data_dict.values() if not data["dividends"].empty]
)
output_file_dividend = "D:/dividend.xlsx"
b = combined_dividends.reset_index()
b.to_csv("D:/dividend.txt", sep="\t", index=False)
b.to_excel(output_file_dividend, index=False, engine="openpyxl")


combined_historical["date"] = pd.to_datetime(
    combined_historical["Date"], errors="coerce"
).dt.tz_localize(None)

a = combined_historical.reset_index()
a.to_csv("D:/prices.txt", sep="\t", index=False)
