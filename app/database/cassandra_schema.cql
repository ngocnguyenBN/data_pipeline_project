CREATE KEYSPACE IF NOT EXISTS my_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};

-- Use the Keyspace
USE my_keyspace;

-- Create Table
CREATE TABLE IF NOT EXISTS imbalance_data (
    ticker TEXT,
    code TEXT,
    date DATE,
    source TEXT,
    codesource TEXT,
    close_und DOUBLE,
    sharesout DOUBLE,
    name TEXT,
    group_by TEXT,
    code_und TEXT,
    name_und TEXT,
    close DOUBLE,
    category TEXT,
    size TEXT,
    market TEXT,
    updated TIMESTAMP,
    type TEXT,
    PRIMARY KEY ((ticker, date), code)
);
