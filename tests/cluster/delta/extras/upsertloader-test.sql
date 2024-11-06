CREATE DATABASE IF NOT EXISTS {UpsertLoaderDb}
COMMENT "Contains Incremental Base test data";

CREATE TABLE IF NOT EXISTS {UpsertLoaderDummy}
(
    col1 INTEGER,
    col2 INTEGER,
    col3 STRING
)
USING DELTA
COMMENT "Contains UpsertLoader test data";

CREATE TABLE IF NOT EXISTS {UpsertLoaderStreamingSource}
(
    col1 INTEGER,
    col2 INTEGER,
    col3 STRING
)
USING DELTA
COMMENT "Contains streaming UpsertLoader test data";


CREATE TABLE IF NOT EXISTS {UpsertLoaderStreamingTarget}
(
    col1 INTEGER,
    col2 INTEGER,
    col3 STRING
)
USING DELTA
COMMENT "Contains streaming UpsertLoader test data";
