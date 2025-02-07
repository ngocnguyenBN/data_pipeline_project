from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import codecs
import os
import sys
from time import sleep
from bs4 import BeautifulSoup
import pandas as pd

filename = "D:/temp.txt"
# service = Service(executable_path=r'E:\Python\IFRC\chromedriver_win32')
# s=Service(ChromeDriverManager().install())
# pCode = "IBM"
# url = f"https://finance.yahoo.com/quote/{pCode}/"

url = "https://www.nyse.com/quote/XNYS:PFE/QUOTE"
options = webdriver.ChromeOptions()
s = Service()

options.add_argument("--headless")
options.add_argument("--window-position=-2400,-2400")

driver = webdriver.Chrome(service=s, options=options)
driver.get(url)  # Open Chrome browser with the specified URL
sleep(1)

# Ensure the directory exists
directory = os.path.dirname(filename)
if not os.path.exists(directory):
    os.makedirs(directory)

# Open file in write mode with encoding
file = codecs.open(filename, "w", "utf-8")

# Obtain page source
pageSource = driver.find_element(
    By.XPATH,
    '//*[@id="integration-id-47e97ed"]/div[1]/div[2]/div[4]/div/div/div/div[3]/div[1]/div[4]/div[3]/div/div/div[1]',
).get_attribute("outerHTML")

pageSource
# Parse the XML
# soup = BeautifulSoup(pageSource, "xml")

soup = BeautifulSoup(pageSource, "html.parser")
divs = soup.find_all("div")

data = [
    div.get_text(strip=True)
    for div in soup.find_all(
        "div", class_=["data-table-header-cell", "data-table-header-cell-price-align"]
    )
]

df = pd.DataFrame(data, columns=["Header"])

data_text = driver.find_element(
    By.XPATH,
    '//*[@id="integration-id-47e97ed"]/div[1]/div[2]/div[4]/div/div/div/div[3]/div[1]/div[4]/div[3]/div/div/div[2]',
).get_attribute("outerHTML")

dt = BeautifulSoup(data_text, "html.parser")
rows = []

# Loop through each row (flex_tr)
for row in dt.find_all("div", class_="flex_tr"):
    # Extract each cell text within the row
    date = row.find("div", class_="Time").get_text(strip=True)
    open_price = row.find("div", class_="Open").get_text(strip=True)
    high_price = row.find("div", class_="High").get_text(strip=True)
    low_price = row.find("div", class_="Low").get_text(strip=True)
    close_price = row.find("div", class_="Close").get_text(strip=True)
    volume = row.find("div", class_="Volume").get_text(strip=True)

    # Append the row data as a list
    rows.append([date, open_price, high_price, low_price, close_price, volume])

# Convert list of rows into a DataFrame
df = pd.DataFrame(rows, columns=["Date", "Open", "High", "Low", "Close", "Volume"])


file.write(pageSource)

# Close file and browser
file.close()
driver.quit()
