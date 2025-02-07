import yfinance as yf
import pandas as pd
import os
from requests.exceptions import ChunkedEncodingError  # Correct import

file_path = 'D:/list_Code_USA.txt'

# Function to split list into chunks
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# Open the file and read the content
with open(file_path, 'r') as file:
    # Skip the first line (column name)
    next(file)

    # Read the rest of the lines and strip any leading/trailing whitespace
    codes_list = [line.strip() for line in file]

# Define output directory for saving Excel files
output_dir = 'D:/Excel_Files_3'
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

# Initialize a counter for naming the files
file_counter = 1

# Process the list in chunks of 100 codes
for code_chunk in chunks(codes_list, 100):
    # Initialize an empty list to collect the data
    data = []
    print(f'Processing chunk {file_counter}...')

    for ticker in code_chunk:
        try:
            # Fetch ticker information
            t = yf.Ticker(ticker)
            info = t.info

            # Add ticker code to the info dictionary
            info['Code'] = ticker

            # Append the info dictionary to the data list
            data.append(info)
        except ChunkedEncodingError:
            print(f"Error fetching data for {ticker}. Skipping...")
        except Exception as e:
            # Catch any other exceptions
            print(f"An error occurred with {ticker}: {e}. Skipping...")

    # Convert the list of dictionaries to a DataFrame
    df = pd.DataFrame(data)

    if not df.empty:
        # Reorder columns if needed, e.g., placing 'Code' as the first column
        columns = ['Code'] + [col for col in df.columns if col != 'Code']
        df = df[columns]

        # Save the DataFrame to an Excel file
        output_file = os.path.join(output_dir, f'ticker_info_part_{file_counter}.xlsx')
        df.to_excel(output_file, index=False, engine='openpyxl')

        print(f"Data saved to {output_file}")

    # Increment the file counter for the next batch
    file_counter += 1

print("Processing complete.")
