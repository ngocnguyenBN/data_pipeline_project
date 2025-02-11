CREATE KEYSPACE IF NOT EXISTS finance
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3};

USE finance;

-- Bảng lưu giá cổ phiếu theo thời gian
CREATE TABLE IF NOT EXISTS data_prices_history (
    ticker TEXT,
    date TIMESTAMP,
    source TEXT,
    open DECIMAL,
    high DECIMAL,
    low DECIMAL,
    close DECIMAL,
    close_adj DECIMAL
    volume BIGINT,
    PRIMARY KEY (ticker, date)
) WITH CLUSTERING ORDER BY (date DESC);

-- Bảng lưu dữ liệu báo cáo tài chính
CREATE TABLE IF NOT EXISTS data_financial_reports (
    ticker TEXT,
    report_date TIMESTAMP,
    revenue DECIMAL,
    net_income DECIMAL,
    eps DECIMAL,
    assets DECIMAL,
    liabilities DECIMAL,
    equity DECIMAL,
    PRIMARY KEY (ticker, report_date)
) WITH CLUSTERING ORDER BY (report_date DESC);
