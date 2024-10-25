CREATE DATABASE IF NOT EXISTS {DeleteDataLoaderDb}
COMMENT "Contains Incremental Base test data";

CREATE TABLE IF NOT EXISTS {DeleteDataLoaderDummy}
(
    col1 INTEGER,
    col2 FLOAT,
    col3 STRING,
    col4 TIMESTAMP
)
USING DELTA
COMMENT "Contains DeleteDataLoader Base test data"
