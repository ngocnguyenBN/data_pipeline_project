from cassandra.cluster import Cluster
import pandas as pd
import concurrent.futures
from cassandra.concurrent import execute_concurrent_with_args


import subprocess

cql_script_local = "D:/data_pipeline_project/app/database/cassandra_schema.cql"
cql_script_container = "/cassandra_schema.cql"
cassandra_container = "cassandra-1"  # Tên container Cassandra trong Docker

# 🔹 1. Copy file `.cql` từ máy host vào container Docker
subprocess.run(
    ["docker", "cp", cql_script_local, f"{cassandra_container}:{cql_script_container}"],
    check=True,
)

# 🔹 2. Chạy cqlsh bên trong container để thực thi script
subprocess.run(
    [
        "docker",
        "exec",
        "-i",
        cassandra_container,
        "cqlsh",
        "172.19.0.3",
        "-f",
        cql_script_container,
    ],
    check=True,
)

print("✅ CQL script executed successfully inside Docker!")

print("✅ CQL script executed successfully!")

# Kết nối tới Cassandra
cluster = Cluster(["localhost"], port=9042)
session = cluster.connect()

# Sử dụng keyspace
session.set_keyspace("my_keyspace")

# Chuẩn bị câu lệnh insert
insert_query = session.prepare(
    """
    INSERT INTO imbalance_data (
        ticker, code, date, source, codesource, close_und, sharesout, name,
        group_by, code_und, name_und, close, category, size, market, updated, type
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
)

# Đọc dữ liệu CSV
df = pd.read_csv(
    "./resources/data_imbalance.csv",
    dtype={"category": str},
    parse_dates=[
        "date",
        "updated",
    ],  # Chuyển đổi sang datetime để phù hợp với Cassandra
    low_memory=False,
)

# Chia nhỏ dữ liệu
batch_size = 5000
df_batches = [df[i : i + batch_size] for i in range(0, len(df), batch_size)]


# # Hàm insert batch tối ưu hơn (giữ nguyên cách cũ)
# def insert_batch(batch):
#     for _, row in batch.iterrows():
#         try:
#             # Chuyển đổi kiểu dữ liệu đúng với Cassandra
#             date_value = row["date"].date() if pd.notna(row["date"]) else None
#             updated_value = (
#                 row["updated"].to_pydatetime() if pd.notna(row["updated"]) else None
#             )
#             size_value = str(row["size"]) if pd.notna(row["size"]) else None

#             future = session.execute_async(
#                 insert_query,
#                 (
#                     row["ticker"],
#                     row["code"],
#                     date_value,
#                     row["source"],
#                     row["codesource"],
#                     row["close_und"],
#                     row["sharesout"],
#                     row["name"],
#                     row["group_by"],
#                     row["code_und"],
#                     row["name_und"],
#                     row["close"],
#                     row["category"],
#                     size_value,
#                     row["market"],
#                     updated_value,
#                     row["type"],
#                 ),
#             )
#             future.result()  # Chờ kết quả hoàn thành để tránh lỗi

#         except Exception as e:
#             print(f"❌ Lỗi khi insert {row['ticker']} vào {row['date']}: {e}")


# # Dùng ThreadPoolExecutor để xử lý song song
# with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
#     executor.map(insert_batch, df_batches)


# Tạo danh sách các truy vấn insert
insert_queries = [
    (
        insert_query,
        (
            row["ticker"],
            row["code"],
            row["date"].date(),
            row["source"],
            row["codesource"],
            row["close_und"],
            row["sharesout"],
            row["name"],
            row["group_by"],
            row["code_und"],
            row["name_und"],
            row["close"],
            row["category"],
            str(row["size"]),
            row["market"],
            row["updated"].to_pydatetime(),
            row["type"],
        ),
    )
    for _, row in df.iterrows()
]

# Chạy insert song song với tối đa 10 luồng
execute_concurrent_with_args(session, insert_queries, concurrency=10)

print("✅ Dữ liệu đã được insert thành công!")

# Kiểm tra dữ liệu
rows = session.execute("SELECT * FROM imbalance_data LIMIT 10")
for row in rows:
    print(row)

# Đóng kết nối
cluster.shutdown()

# from cassandra.auth import PlainTextAuthProvider
# import uuid

# # py -3.11 -m venv venv
# # Define connection settings
# cassandra_host = "localhost"  # Docker runs it on localhost
# cassandra_port = 9042
# username = "admin"
# password = "admin"

# # Connect to the Cassandra cluster
# auth_provider = PlainTextAuthProvider(username, password)
# cluster = Cluster([cassandra_host], port=cassandra_port, auth_provider=auth_provider)
