from fastapi import FastAPI
from utilities import (
    get_exness_mt5_accounts,
    get_vantage_mt5_accounts,
    get_current_equity,
    get_current_balance,
)
from crawlers import MT5DataSource
import os

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/cron/sync-realtime-equity")
async def sync_realtime_equity():
    exness_accounts = get_exness_mt5_accounts()
    vantage_accounts = get_vantage_mt5_accounts()

    exness_terminal_path = os.environ.get("MT5_TERMINAL_EXNESS_PATH")
    vantage_terminal_path = os.environ.get("MT5_TERMINAL_VANTAGE_PATH")

    MT5DataSource().sync_broker_accounts(exness_accounts, exness_terminal_path)
    MT5DataSource().sync_broker_accounts(vantage_accounts, vantage_terminal_path)

    MT5DataSource().sync_realtime_equity(exness_accounts, exness_terminal_path)
    MT5DataSource().sync_realtime_equity(vantage_accounts, vantage_terminal_path)
    return {"status": True, "msg": "Success"}


@app.get("/dapp/nav")
async def dapp_nav():
    return {
        "status": True,
        "data": {
            "balance": get_current_balance(),
            "equity": get_current_equity(),
        },
    }
