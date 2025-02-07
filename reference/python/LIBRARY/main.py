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


if __name__ == "__main__":
    from python_library import *  # This is fine in a package, relative import works

    file_path = "D:/python_r_DA/list_file/list_stkvn.txt"
    with open(file_path, "r") as file:
        next(file)

        codes_list = [line.strip() for line in file]

    dataa = download_imbalance_data_by_list(list_codes=codes_list, do_history=False)
