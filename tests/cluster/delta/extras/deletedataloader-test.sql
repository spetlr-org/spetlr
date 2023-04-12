CREATE DATABASE IF NOT EXISTS {DeleteDataLoaderDb}
COMMENT "Contains Incremental Base test data"
LOCATION "{DeleteDataLoaderDb_path}";

CREATE TABLE IF NOT EXISTS {DeleteDataLoaderDummy}
(
    col1 INTEGER,
    col2 FLOAT,
    col3 TIMESTAMP
    col4 NVARCHAR(255)
)
USING DELTA
COMMENT "Contains DeleteDataLoader Base test data"
LOCATION "{DeleteDataLoaderDummy_path}"
