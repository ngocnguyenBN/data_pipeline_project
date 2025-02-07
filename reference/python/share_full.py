import yfinance as yf
import pandas as pd
from datetime import datetime

# Define the list of codes
# codes = ['AAPL']  # Example with one code
with open( "D:/list_Code_USA.txt", 'r') as f:
    codes = f.read().splitlines()
# Initialize an empty list to store the results
all_data = []

# Get the current date for the 'end' parameter
end_date = datetime.today().strftime('%Y-%m-%d')

# Loop through the codes and fetch the shares data
for code in codes:
    stock = yf.Ticker(code)
    try:
        share_data = stock.get_shares_full(start="1970-01-01", end=end_date)
        if share_data is not None:
            share_data = share_data.reset_index()  # Convert index to column if needed
            share_data.columns = ["Date", "Shares"]
            share_data['codesource'] = code
            # Append data to the list
            all_data.append(share_data[['codesource', 'Date', 'Shares']])
    except Exception as e:
        print(f"Error for code {code}: {e}")

# Concatenate all the data into a single data frame
if all_data:
    final_data = pd.concat(all_data)

    # Rename the columns to match your desired output
    final_data.columns = ["codesource", "date", "sharesout"]

    # Display or save the final result
    print(final_data)
else:
    print("No data fetched.")

final_data['date'] = pd.to_datetime(final_data['date']).dt.tz_localize(None)

file = 'D:/data_shares_sp500_history.xlsx'
final_data.to_excel(file, index=False, engine='openpyxl')


final_data.to_csv('D:/data_shares_sp500_history.txt', sep="\t", index=False)