from pathlib import Path
from flask import jsonify
import os
import pandas as pd
from crawlers import MT5DataSource

RESOURCES_DIR = Path(__file__).resolve().parent.parent.parent / "resources"


def get_exness_mt5_accounts():
    CSV_PATH = RESOURCES_DIR / "exness-mt5-accounts.csv"

    df = pd.read_csv(CSV_PATH, header=None)
    # data = df.to_dict(orient='split')['data']

    # print('data', data)
    # flattened_data = [item for sublist in data for item in sublist]
    # convert_to_objects = [
    #     {"server": item.split(";")[0], "account_id": item.split(";")[1], "password": item.split(";")[2]}
    #     for item in flattened_data
    # ]
    df.columns = ["server", "account_id", "password"]
    convert_to_objects = df.to_dict(orient="records")
    return convert_to_objects


def get_vantage_mt5_accounts():
    CSV_PATH = RESOURCES_DIR / "vantage-mt5-accounts.csv"

    df = pd.read_csv(CSV_PATH, header=None)
    # data = df.to_dict(orient='split')['data']

    # print('data', data)
    # flattened_data = [item for sublist in data for item in sublist]
    # convert_to_objects = [
    #     {"server": item.split(";")[0], "account_id": item.split(";")[1], "password": item.split(";")[2]}
    #     for item in flattened_data
    # ]
    df.columns = ["server", "account_id", "password"]
    convert_to_objects = df.to_dict(orient="records")
    return convert_to_objects
