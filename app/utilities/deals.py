from database import engine, HistoryDeals
from sqlalchemy.orm import sessionmaker
from sqlalchemy import and_, desc
from datetime import datetime, timedelta, timezone, time, date

def get_current_balance():
    Session = sessionmaker(bind=engine)
    session = Session()
    records_today = session.query(HistoryDeals).filter_by(**{
        "timestamp_iso" : date.today().isoformat()
    }).all()
    records_today_dict = [row.to_dict() for row in records_today]
    print('records_today_dict',records_today_dict)
    return sum(x['account_balance'] for x in records_today_dict)


def get_current_equity():
    Session = sessionmaker(bind=engine)
    session = Session()
    records_today = session.query(HistoryDeals).filter_by(**{
        "timestamp_iso" : date.today().isoformat()
    }).all()
    records_today_dict = [row.to_dict() for row in records_today]
    return sum(x['account_equity'] for x in records_today_dict)