import MetaTrader5 as mt5
import pandas as pd
from datetime import datetime


# Log in to the demo account
account = 5033013189  # Replace with your demo account number
password = "8-WwAaIk"  # Replace with your demo account password
server = "MetaQuotes-Demo"  # Replace with your broker's server name

mt5.initialize()
mt5.login(account, password, server)
account_info = mt5.account_info()
print("....", account_info._asdict())
mt5.shutdown()

if account_info is None:
    print("Failed to retrieve account info:", mt5.last_error())

# FETCH Account INFO
account_info_dict = account_info._asdict()
print("account_info_dict", account_info_dict)
# Shutdown the MT5 terminal
