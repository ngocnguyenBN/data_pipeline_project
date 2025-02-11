from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid

# py -3.11 -m venv venv
# Define connection settings
cassandra_host = "localhost"  # Docker runs it on localhost
cassandra_port = 9042
username = "admin"
password = "admin"

# Connect to the Cassandra cluster
auth_provider = PlainTextAuthProvider(username, password)
cluster = Cluster([cassandra_host], port=cassandra_port, auth_provider=auth_provider)
session = cluster.connect()

# # Create a keyspace
# session.execute(
#     """
#     CREATE KEYSPACE IF NOT EXISTS my_keyspace
#     WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
# """
# )

# Use the keyspace
session.set_keyspace("my_keyspace")

# # Create a table
session.execute(
    """
    CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY,
        name TEXT,
        age INT
    )
"""
)


# Insert data
session.execute(
    """
    INSERT INTO users (id, name, age) VALUES (%s, %s, %s)
""",
    (uuid.uuid4(), "ngoc", 30),
)

# Query data
rows = session.execute("SELECT * FROM users")
for row in rows:
    print(row)

# print("Table created successfully!")

# Close the connection
cluster.shutdown()
