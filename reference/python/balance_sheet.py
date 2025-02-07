import yfinance as yf
import pandas as pd

file_path = "S:/R/DATA/list_code_wceo.txt"
# Open the file and read the content
with open(file_path, "r") as file:
    # Skip the first line (column name)
    next(file)
    # Read the rest of the lines and strip any leading/trailing whitespace
    codes_list = [line.strip() for line in file]

# Print the list to verify
print(codes_list)

all_data = []  # List để chứa dữ liệu từng mã
for code in codes_list:

    ticker = yf.Ticker(code)
    balance_sheet = ticker.income_stmt
    balance_sheet = balance_sheet.reset_index()
    balance_sheet.rename(columns={balance_sheet.columns[0]: "Metric"}, inplace=True)
    balance_sheet["codesource"] = code
    all_data.append(balance_sheet)  # Thêm dữ liệu của từng mã vào danh sách

combined_data = pd.concat(all_data, ignore_index=True)
# combined_data.to_csv("D:/balance_sheets_wceo.txt", sep="\t", index=False)
combined_data.to_csv("D:/income_stat_wceo.txt", sep="\t", index=False)

print("Dữ liệu đã được lưu vào file balance_sheets.txt")
