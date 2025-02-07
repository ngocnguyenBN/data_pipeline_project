import sys

sys.path.append("app")
# from crawlers.mt5 import MT5DataSource

# # Initialize the MT5DataSource
# mt5_source = MT5DataSource()

# try:
#     # Step 1: Connect to the MT5 server
#     mt5_source.connect()

#     # Step 2: Pull data
#     raw_data = mt5_source.pull_data()
#     print("Raw data:", raw_data)

#     # Step 3: Clean data
#     cleaned_data = mt5_source.clean_data(raw_data)
#     print("Cleaned data:", cleaned_data)

# finally:
#     # Step 4: Close the connection
#     mt5_source.close_connection()
# # pip freeze > requirements.txt
# #  "ccpr.vn"             = {lmy.connexion = try(dbConnect(MySQL(), user = "ccpr_user",       password = "Ifrc@2023_ccpr",    host = '27.71.235.40', dbname = "ifrc_ccpr"))},
# #  "ifrc.vn"             = {lmy.connexion = try(dbConnect(MySQL(), user = "dbifrc",          password = "@ifrcDB2022",       host = '27.71.235.71', dbname = "db_ifrcvn"))},
# #  "datacenter.ifrc.vn"  = {lmy.connexion = try(dbConnect(MySQL(), user = "datacenter_user", password = "@ifrcDB2022",       host = '27.71.235.71', dbname = "wp_datacenter"))},
# #  "board.womenceoworld" = {lmy.connexion = try(dbConnect(MySQL(), user = "womenworld_user", password = "admin@beqholdings", host = '27.71.235.71', dbname = "db_womenceoworld"))},
# #  "ccpi_dashboard"      = {lmy.connexion = try(dbConnect(MySQL(), user = "dashboard_user", password = "admin@beqholdings", host = '27.71.235.71', dbname = "ccpi_dashboard"))},
# #  "db_ccpe"             = {lmy.connexion = try(dbConnect(MySQL(), user = "ccpevn_user", password = "admin@beqholdings", host = '27.71.235.40', dbname = "db_ccpe"))},
# #  "dashboard_live_dev"  = {lmy.connexion = try(dbConnect(MySQL(), user = "dashboard_user_login_data", password = "admin@beqholdings", host = '27.71.235.71', dbname = "dashboard_user_login_dev"))},
# #  'dashboard_live'      = {lmy.connexion = try(dbConnect(MySQL(), user = "dashboard_user_login", password = "admin@beqholdings", host = '27.71.235.71', dbname = "ccpi_dashboard_login"))},
# #  'dashboard_live_bat'  = {lmy.connexion = try(dbConnect(MySQL(), user = "dashboard_user_login_yen", password = "yen@beqholdings", host = '27.71.235.71', dbname = "ccpi_dashboard_login"))},
# #  'dashboard_lite'      = {lmy.connexion = try(dbConnect(MySQL(), user = "ccpi_dashboardlite_user", password = "admin@beqholdings", host = '27.71.235.71', dbname = "ccpi_dashboardlite"))},

# import psycopg2
# import mysql.connector

# # Connect to PostgreSQL
# # conn = psycopg2.connect(
# #     dbname="ccpi_dashboard_login",
# #     user="dashboard_user_login",
# #     password="admin@beqholdings",
# #     host="27.71.235.71",
# #     # port="5432",
# # )
# conn = mysql.connector.connect(
#     host="27.71.235.71",
#     user="dashboard_user_login",
#     password="admin@beqholdings",
#     database="ccpi_dashboard_login",
# )
# cursor = conn.cursor()

# # Execute a query
# cursor.execute("SELECT * FROM dbl_ind_performance")
# # Fetch and print results
# for row in cursor.fetchall():
#     print(row)
# conn.close()

import os
from app.crawlers.yahoofinance_crawler import YAHDataSource

# from .yahoofinance_crawler import YAHDataSource

crawler = YAHDataSource("AAPL")
prices = crawler.get_data_ohlc()
prices.show(5)