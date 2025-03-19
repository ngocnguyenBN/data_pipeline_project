import requests
import pandas as pd
import datetime
import json
import re


def get_businessinsider_code(
    page_url="http://markets.businessinsider.com/index/dow_jones",
):
    response = requests.get(page_url)
    page_str = response.text

    page_info_match = re.search(
        r"var detailChartViewmodel(.*?)</script>", page_str, re.DOTALL
    )
    page_info = page_info_match.group(1) if page_info_match else ""
    if not page_info:
        return pd.DataFrame()

    bin_code = {
        "instrumentname": (
            re.search(r'"InstrumentName" : "(.*?)"', page_info).group(1)
            if re.search(r'"InstrumentName" : "(.*?)"', page_info)
            else None
        ),
        "instrumenttype": (
            re.search(r'"InstrumentType" : "(.*?)"', page_info).group(1)
            if re.search(r'"InstrumentType" : "(.*?)"', page_info)
            else None
        ),
        "instrumentid": (
            re.search(r'"InstrumentTypeID" : (.*?),', page_info).group(1)
            if re.search(r'"InstrumentTypeID" : (.*?),', page_info)
            else None
        ),
        "chartsource": (
            re.search(r'"ChartSource" : "(.*?)"', page_info).group(1)
            if re.search(r'"ChartSource" : "(.*?)"', page_info)
            else None
        ),
        "chartexchange": (
            re.search(r'"ChartExchange" : "(.*?)"', page_info).group(1)
            if re.search(r'"ChartExchange" : "(.*?)"', page_info)
            else None
        ),
        "chartvalor": (
            re.search(r'"ChartValor" : "(.*?)"', page_info).group(1)
            if re.search(r'"ChartValor" : "(.*?)"', page_info)
            else None
        ),
        "chartcurrency": (
            re.search(r'"ChartCurrency" : "(.*?)"', page_info).group(1)
            if re.search(r'"ChartCurrency" : "(.*?)"', page_info)
            else None
        ),
        "tkdata": (
            re.search(r'"TKData" : "(.*?)"', page_info).group(1)
            if re.search(r'"TKData" : "(.*?)"', page_info)
            else None
        ),
        "intradaytktata": (
            re.search(r'"intradayTkData" : "(.*?)"', page_info).group(1)
            if re.search(r'"intradayTkData" : "(.*?)"', page_info)
            else None
        ),
    }

    return pd.DataFrame([bin_code])


def download_bin_by_code(p_codesource="index/dow_jones", nb_back=0):
    print(f"Downloading BIN data for: {p_codesource}")

    bin_code = get_businessinsider_code(
        f"http://markets.businessinsider.com/{p_codesource}"
    )
    if bin_code.empty:
        return pd.DataFrame()

    ptk_data = bin_code.iloc[0]["tkdata"]
    p_type = bin_code.iloc[0]["instrumenttype"]

    if nb_back == 0:
        date_from = "19701231"
    else:
        date_from = (datetime.date.today() - datetime.timedelta(days=nb_back)).strftime(
            "%Y%m%d"
        )

    date_to = datetime.date.today().strftime("%Y%m%d")

    p_url_day = (
        f"http://markets.businessinsider.com/Ajax/Chart_GetChartData?"
        f"instrumentType={p_type}&tkData={ptk_data}&from={date_from}&to={date_to}"
    )

    print(f"Fetching data from: {p_url_day}")

    response = requests.get(p_url_day)
    print(response)
    if response.status_code != 200:
        return pd.DataFrame()

    try:
        dt_day = pd.DataFrame(json.loads(response.text))
        dt_day["Date"] = pd.to_datetime(dt_day["Date"])
        dt_day["codesource"] = p_codesource
        dt_day["source"] = "BIN"
        return dt_day
    except:
        return pd.DataFrame()
