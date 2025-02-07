import pandas as pd
import eikon as ek
import time
from datetime import datetime, timedelta
import os
import random  # Thư viện để lấy mẫu ngẫu nhiên
from pathlib import Path


column_mapping = {
    "Instrument": "codesource",
    "Price Open": "open",
    "Price High": "high",
    "Price Low": "low",
    "Calc Date": "date",
    "Close Price (Unadjusted)": "close",
    "Close Price (Adjusted)": "close_adj",
    "Volume": "volume",
    "Outstanding Shares": "share_outstanding",
    "Free Float": "float",
    "Company Market Cap": "market_cap",
    "Daily Value Traded": "turnover",
    "Currency": "cur",
    "Daily Total Return": "rt",
    "Listed": "shareslis",
    "Launch Date": "launchdate",
}

# FIELD to get
fields = [
    "TR.PriceOpen",
    "TR.PriceHigh",
    "TR.PriceLow",
    "TR.PRICECLOSEDATE.CALCDATE",
    "TR.CLOSEPRICE(ADJUSTED=0)",
    "TR.CLOSEPRICE(ADJUSTED=1)",
    "TR.Volume",
    "TR.SharesOutstanding",
    "TR.SHARESFREEFLOAT",
    "TR.COMPANYMARKETCAP",
    "TR.DailyValueTraded",
    "TR.PriceClose.Currency",
    "TR.TotalReturn1D",
    "TR.SharesListed",
]

dtypes = {
    "code": "string",
    "codesource": "string",
    "source": "string",
    "shareslis": "float64",
    "share_outstanding": "float64",
    "float": "float64",
    "close": "float64",
    "close_adj": "float64",
    "rt": "float64",
    "market_cap": "float64",
    "volume": "float64",
    "turnover": "float64",
    "cur": "string",
    "name": "string",
    "sector_code": "string",
    "type": "string",
    "market": "string",
    "updated": "string",  # Nếu cần datetime, bạn có thể chuyển đổi sau
}

# excel_file = "D:/list_etfdb.xlsx"
excel_file = "S:/CCPR/DATA/LIST/list_etfdb.xlsx"
lock_save_file = Path("D:/waiting_saving.lock")
lock_save_file_history = Path("D:/waiting_saving_history.lock")

# __name__ = "__main__"
if __name__ == "__main__":
    ek.set_app_key("0e5ab9d05f624c0eb37fbf17af88d96502cabda8")

    # File paths
    file_history = "S:/CCPR/DATA/EIK/download_etf_history_new.csv"
    file_day = "S:/CCPR/DATA/EIK/download_etf_prices_new.csv"
    sys_date = datetime.now().strftime("%Y%m%d")
    file_date = f"S:/CCPR/DATA/EIK/download_etf_prices_new_{sys_date}.csv"

    file_history_summary = (
        "S:/CCPR/DATA/IFRCBEQ/DOWNLOAD/IFRCBEQ_EIK_ETF_HISTORY_summary.txt"
    )
    dt_summary = pd.read_csv(
        file_history_summary, delimiter="\t"
    )  # Adjust delimiter if necessary
    # dt_summary = dt_summary.drop_duplicates(subset=['codesource'])
    # columns_to_show = ["code", "start", "date", "nb"]
    # df_selected = dt_summary[columns_to_show]
    # Determine start date
    old_data = pd.DataFrame()
    start_date = "2008-01-01"
    end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    # Load ETF list
    etf_ref = pd.read_excel(excel_file)

    input_list = etf_ref["eik"].dropna().tolist()

    if len(input_list) > 0:
        split_lists = [input_list[i : i + 100] for i in range(0, len(input_list), 100)]
        final_data = pd.DataFrame()

        for k, sublist in enumerate(split_lists, start=1):
            lock_file = Path(f"D:/waiting_{k}.lock")

            # Check if lock file exists
            if lock_file.exists():
                lock_age = time.time() - lock_file.stat().st_mtime
                if lock_age > 600:  # Lock older than 10 minutes
                    print(f"Lock file exists too long, removing: {lock_file}")
                    lock_file.unlink()

            # Create lock file if it doesn't exist
            if not lock_file.exists():
                lock_file.touch()
                try:
                    x_list = []
                    for code in sublist:
                        try:
                            print(code)
                            if code in dt_summary["codesource"].values:
                                row = dt_summary[dt_summary["codesource"] == code].iloc[
                                    0
                                ]
                                if row["nb"] <= 250:
                                    start_date = "2008-01-01"
                                else:
                                    start_date = (
                                        datetime.now() - timedelta(days=30)
                                    ).strftime("%Y-%m-%d")
                            else:
                                start_date = "2008-01-01"

                            data, _ = ek.get_data(
                                [code], fields, {"SDate": start_date, "EDate": end_date}
                            )
                            data["symbol"] = code
                            data.columns.values[5] = "Close Price (Unadjusted)"
                            data.columns.values[6] = "Close Price (Adjusted)"

                            launch_date, _ = ek.get_data(
                                [code],
                                ["TR.FundLaunchDate"],
                                {"SDate": start_date, "EDate": end_date},
                            )
                            launch_date_value = launch_date["Launch Date"].iloc[0]
                            data["Launch Date"] = launch_date_value
                            print(data)

                            x_list.append(data)
                        except Exception as e:
                            print(f"Error processing code {code}: {e}")

                    if x_list:
                        combined_data = pd.concat(x_list, ignore_index=True)

                        if not combined_data.empty:
                            all_data = combined_data
                            all_data.rename(columns=column_mapping, inplace=True)
                            all_data["source"] = "EIK"
                            all_data["type"] = "FND"

                            # Merge with ETF reference
                            etf_ref_filtered = etf_ref[etf_ref["eik"].notna()]
                            etf_ref_filtered = etf_ref_filtered.rename(
                                columns={
                                    "eik": "codesource",
                                    "etf_name": "name",
                                    "symbol": "sector_code",
                                }
                            )
                            etf_ref_filtered = etf_ref_filtered[
                                ["codesource", "name", "sector_code", "code"]
                            ]
                            all_data = all_data.merge(
                                etf_ref_filtered, on="codesource", how="left"
                            )
                            all_data["updated"] = datetime.now().strftime(
                                "%Y-%m-%d %H:%M:%S"
                            )

                        if not os.path.exists(file_date):
                            old_data = pd.DataFrame()
                        else:
                            # old_data = pd.read_csv(
                            #     file_date,
                            #     dtype=dtypes,
                            #     parse_dates=["date"],
                            #     usecols=lambda col: not col.startswith("Unnamed"),
                            # )
                            old_data = pd.read_csv(
                                file_date,
                                usecols=lambda col: not col.startswith("Unnamed"),
                            )
                            missing_cols = set(dtypes.keys()) - set(old_data.columns)
                            for col in missing_cols:
                                old_data[col] = np.nan

                            old_data = old_data.astype(dtypes)

                            old_data = old_data.loc[
                                :, ~old_data.columns.str.contains("^Unnamed")
                            ]

                        final_data = pd.concat(
                            [combined_data, old_data], ignore_index=True
                        ).drop_duplicates(subset=["codesource", "date"])

                        while os.path.exists(lock_save_file):
                            time.sleep(30)
                        else:
                            lock_save_file.touch()
                            final_data.to_csv(file_date, index=False)
                            final_data.to_csv(file_day, index=False)
                            lock_save_file.unlink()

                finally:
                    lock_file.unlink()

            if not os.path.exists(file_history):
                history = pd.DataFrame()
            else:
                # history = pd.read_csv(
                #     file_history,
                #     dtype=dtypes,
                #     parse_dates=["date"],
                #     usecols=lambda col: not col.startswith("Unnamed"),
                # )
                history = pd.read_csv(
                    file_history,
                    usecols=lambda col: not col.startswith("Unnamed"),
                )
                missing_cols = set(dtypes.keys()) - set(history.columns)
                for col in missing_cols:
                    history[col] = np.nan

                history = history.astype(dtypes)

                history = history.loc[:, ~history.columns.str.contains("^Unnamed")]

            if not os.path.exists(file_date):
                dt_date = pd.DataFrame()
            else:
                dt_date = pd.read_csv(
                    file_date,
                    usecols=lambda col: not col.startswith("Unnamed"),
                )
                missing_cols = set(dtypes.keys()) - set(dt_date.columns)
                for col in missing_cols:
                    dt_date[col] = np.nan

                dt_date = dt_date.astype(dtypes)
                dt_date = dt_date.loc[:, ~dt_date.columns.str.contains("^Unnamed")]

            history = pd.concat([dt_date, history], ignore_index=True).drop_duplicates(
                subset=["codesource", "date"]
            )

            if not history.empty:
                while os.path.exists(lock_save_file_history):
                    time.sleep(30)
                else:
                    lock_save_file_history.touch()
                    final_data.to_csv(file_date, index=False)
                    final_data.to_csv(file_day, index=False)
                    lock_save_file_history.unlink()
