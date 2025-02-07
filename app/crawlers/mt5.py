from .base import DataSource
import MetaTrader5 as mt5
from MetaTrader5 import DEAL_TYPE_BUY, DEAL_TYPE_SELL, DEAL_TYPE_BALANCE
from datetime import datetime, timedelta, timezone, time, date
import subprocess
import json
import pytz
from database import engine, HistoryDeals, BrokerAccounts
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, desc
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP


class MT5DataSource(DataSource):
    def __init__(self):
        Session = sessionmaker(bind=engine)
        session = Session()
        self._db_session = session
        pass

    def pull_data(self):
        # Fetch account information
        account_info = mt5.account_info()
        account_info_dict = account_info._asdict()

        history_orders_dict = []
        history_deals_dict = []

        from_date = datetime(2015, 1, 1)
        to_date = datetime.now() + timedelta(days=1)

        history_orders = mt5.history_orders_get(from_date, to_date)
        if history_orders:
            history_orders_dict = [order._asdict() for order in history_orders]

        history_deals = mt5.history_deals_get(from_date, to_date)
        if history_deals:
            history_deals_dict = [deal._asdict() for deal in history_deals]

        # Fetch open positions
        positions = mt5.positions_get()
        positions_list = []
        if positions:
            for pos in positions:
                positions_list.append(pos._asdict())

        # Fetch pending orders
        orders = mt5.orders_get()
        orders_list = []
        if orders:
            for order in orders:
                orders_list.append(order._asdict())

        return {
            "account": account_info_dict,
            "positions": positions_list,
            "orders": orders_list,
            "history_orders": history_orders_dict,
            "history_deals": history_deals_dict,
        }

    def connect(self):
        return super().connect()

    def get_all_raw_data(self, data):
        # Fetch account information
        account_info = data
        account_info_dict = account_info._asdict()

        history_orders_dict = []
        history_deals_dict = []

        from_date = datetime(2020, 1, 1)
        to_date = datetime.now() + timedelta(days=1)

        history_orders = mt5.history_orders_get(from_date, to_date)
        if history_orders:
            history_orders_dict = [order._asdict() for order in history_orders]

        history_deals = mt5.history_deals_get(from_date, to_date)
        if history_deals:
            history_deals_dict = [deal._asdict() for deal in history_deals]

        # Fetch open positions
        positions = mt5.positions_get()
        positions_list = []
        if positions:
            for pos in positions:
                positions_list.append(pos._asdict())

        # Fetch pending orders
        orders = mt5.orders_get()
        orders_list = []
        if orders:
            for order in orders:
                orders_list.append(order._asdict())

        return {
            "account": account_info_dict,
            "positions": positions_list,
            "orders": orders_list,
            "history_orders": history_orders_dict,
            "history_deals": history_deals_dict,
        }

    def clean_data(self, account_info_dict):
        print("Cleaning MT5 data...")

        cleaned_data = account_info_dict.copy()

        for key_group in [
            "Positions",
            "PendingOrders",
            "history_orders",
            "history_deals",
        ]:
            for item in cleaned_data.get(key_group, []):
                for key in [
                    "time",
                    "time_update",
                    "time_msc",
                    "time_update_msc",
                    "time_done_msc",
                    "time_done",
                    "time_setup",
                    "time_setup_msc",
                ]:
                    if key in item and item[key]:
                        item[key] = self.format_time(item[key])

        orders = cleaned_data.get("history_orders", [])
        deals = cleaned_data.get("history_deals", [])
        trade_history = []

        deal_dict = {}
        for deal in deals:
            position_id = deal.get("position_id")
            if position_id not in deal_dict:
                deal_dict[position_id] = []
            deal_dict[position_id].append(deal)

        order_fields = [
            "time_done_msc",
            "price_open",
            "volume_initial",
            "sl",
            "tp",
            "price_stoplimit",
        ]
        deal_fields = ["time_msc", "price", "volume", "profit"]

        for order in orders:
            position_id = order.get("position_id")
            symbol = order.get("symbol")
            order_type = order.get("type")

            linked_deals = deal_dict.get(position_id, [])

            for deal in linked_deals:
                if deal.get("symbol") == symbol:
                    trade_entry = {
                        "symbol": symbol,
                        "position_id": position_id,
                        "order_type": order_type,  # type = 0 : BUY, type = 1 : SELL
                    }

                    for field in order_fields:
                        trade_entry[f"order_{field}"] = order.get(field)

                    # Add fields from deal
                    for field in deal_fields:
                        trade_entry[f"deal_{field}"] = deal.get(field)

                    if (
                        trade_entry["order_time_done_msc"]
                        == trade_entry["deal_time_msc"]
                    ):
                        continue
                    trade_history.append(trade_entry)

        cleaned_data["trade_history"] = trade_history

        fields_to_exclude = ["history_orders", "history_deals"]

        cleaned_data = {
            key: value
            for key, value in cleaned_data.items()
            if key not in fields_to_exclude and value is not None
        }

        # with open("output.json", "w", encoding="utf-8") as json_file:
        #     json.dump(cleaned_data, json_file, indent=4, ensure_ascii=False)

        return {"data": json.dumps(cleaned_data)}

    def close_connection(self):
        mt5.shutdown()
        print("Closing connection to MT5 server.")

    def sync_raw_data_platform(self, accounts, terminal_path):

        subprocess.run(["taskkill", "/IM", "terminal64.exe", "/F"])

        mt5.initialize(path=terminal_path)

        arr_info = []

        for acc in accounts:
            _result = mt5.login(int(acc["account_id"]), acc["password"], acc["server"])
            if _result:
                arr_info.append(mt5.account_info())

        for data in arr_info:
            data_again = self.get_all_raw_data(data)
            new_log = TrackingDaily(
                account_logs=data_again,  # Dữ liệu JSON
                broker_name=data_again["account"]["company"],
                platform_name="MetaTrader5",
                account_id=data_again["account"]["login"],
            )
            self._db_session.add(new_log)
            self._db_session.commit()

        return {"status": True, "msg": "Update Success"}

    def fetch_data(self, where={}):
        if len(where) > 0:

            logs = self._db_session.query(TrackingDaily).filter_by(**where).all()
            logs_dict = [row.to_dict() for row in logs]
            return {"status": True, "data": logs_dict, "where": where}

        logs = self._db_session.query(TrackingDaily).all()
        logs_dict = [row.to_dict() for row in logs]

        return {
            "status": True,
            "data": logs_dict,
        }

    def find_first(self, where={}):
        if len(where) > 0:
            item = (
                self._db_session.query(TrackingDaily)
                .filter_by(**where)
                .order_by(desc(TrackingDaily.created_at))
                .first()
            )
            if item:
                item_dict = item.to_dict()
                return {"status": True, "data": item_dict, "where": where}

        return {
            "status": False,
            "data": {},
        }

    def format_time(self, timestamp):
        if timestamp and isinstance(timestamp, (int, float)):
            if timestamp > 10000000000:
                timestamp /= 1000
            utc_time = datetime.utcfromtimestamp(timestamp)
            utc_time = pytz.utc.localize(utc_time)
            return utc_time.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return None

    def calculate_balance_at(
        self,
        date_time,
    ):
        time_from = datetime(2015, 1, 1)
        time_from = datetime.combine(date_time, time(0, 0, 0))
        time_to = datetime.combine(date_time, time(23, 59, 59))

        deals = mt5.history_deals_get(time_from, time_to)
        balance = sum(
            [
                deal.profit
                for deal in deals
                if deal.type in [DEAL_TYPE_BUY, DEAL_TYPE_SELL, DEAL_TYPE_BALANCE]
            ]
        )

        return balance

    def fetch_transform_time_series(self, where={}):
        if len(where) > 0:

            logs = self._db_session.query(HistoryDealsSeries).filter_by(**where).all()
            logs_dict = [row.to_dict() for row in logs]
            return {"status": True, "data": logs_dict, "where": where}

        logs = self._db_session.query(HistoryDealsSeries).all()
        logs_dict = [row.to_dict() for row in logs]

        return {"status": True, "data": logs_dict, "where": where}

    def timestamp_to_date(self, unix_time):
        return datetime.fromtimestamp(unix_time, tz=timezone.utc).date()

    def sync_history_deals(self, mt5_data):
        history_deals = mt5_data["history_deals"]
        daily_profit = defaultdict(float)
        count_by_date = defaultdict(int)
        for deal in history_deals:
            deal_date = self.timestamp_to_date(deal["time"])
            daily_profit[deal_date.isoformat()] += deal["profit"]
            count_by_date[deal_date.isoformat()] += 1

        current_balance = 0
        balance_by_date = {}
        for day, profit in sorted(daily_profit.items()):
            current_balance += profit
            number = Decimal(current_balance)
            balance_by_date[day] = float(
                number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            )
            record = HistoryDeals(
                timestamp=datetime.strptime(day, "%Y-%m-%d")
                .replace(tzinfo=timezone.utc)
                .timestamp(),
                timestamp_iso=day,
                account_id=mt5_data["account"]["login"],
                account_balance=balance_by_date[day],
                account_equity=balance_by_date[day],
            )
            self._db_session.merge(record)
            self._db_session.commit()
        pass

    def sync_broker_accounts(self, accounts, terminal_path):
        mt5.initialize(path=terminal_path)

        arr_info = []

        for acc in accounts:
            check_account = (
                self._db_session.query(BrokerAccounts)
                .filter_by(account_id=str(acc["account_id"]))
                .first()
            )

            if check_account is None:
                _result = mt5.login(
                    int(acc["account_id"]), acc["password"], acc["server"]
                )
                if _result:
                    arr_info.append(mt5.account_info())

        for data in arr_info:
            data_again = self.get_all_raw_data(data)
            new_log = BrokerAccounts(
                account_logs=data_again,  # Dữ liệu JSON
                broker_name=data_again["account"]["company"],
                platform_name="MetaTrader5",
                account_id=data_again["account"]["login"],
            )
            self._db_session.add(new_log)
            self._db_session.commit()

        return {"status": True, "msg": "Update Success"}

    def has_yesterday_deal(self, account_id):  # Boolean
        today = date.today().isoformat()
        result = (
            self._db_session.query(HistoryDeals)
            .filter_by(**{"timestamp_iso": today, "account_id": str(account_id)})
            .order_by(desc(HistoryDeals.created_at))
            .first()
        )

        if result == None:
            return False
        return True

    def sync_realtime_equity(self, accounts, terminal_path):

        # subprocess.run(["taskkill", "/IM", "terminal64.exe", "/F"])
        today_timestamp = int(datetime.now().timestamp())
        mt5.initialize(path=terminal_path)

        arr_info = []

        for acc in accounts:
            _result = mt5.login(int(acc["account_id"]), acc["password"], acc["server"])
            if _result:
                arr_info.append(mt5.account_info())

        for data in arr_info:
            data_again = self.get_all_raw_data(data)

            account_info = data_again["account"]
            # check is exist yesterday record in db
            has_yesterday = self.has_yesterday_deal(account_info["login"])
            if has_yesterday == False:
                # sync_history_deals
                self.sync_history_deals(data_again)
                pass

            today_record = (
                self._db_session.query(HistoryDeals)
                .filter_by(
                    **{
                        "timestamp_iso": date.today().isoformat(),
                        "account_id": str(account_info["login"]),
                    }
                )
                .order_by(desc(HistoryDeals.created_at))
                .first()
            )

            if today_record == None:
                # add record
                record = HistoryDeals(
                    timestamp=today_timestamp,
                    timestamp_iso=self.timestamp_to_date(today_timestamp).isoformat(),
                    account_id=str(account_info["login"]),
                    account_balance=account_info["balance"],
                    account_equity=account_info["equity"],
                )
                self._db_session.add(record)
            else:
                # update record
                today_record.account_balance = account_info["balance"]
                today_record.account_equity = account_info["equity"]

            self._db_session.commit()
