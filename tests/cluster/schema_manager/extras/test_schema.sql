CREATE DATABASE IF NOT EXISTS {SchemaTestDb}
COMMENT "Contains schema testing data"
LOCATION "{SchemaTestDb_path}";

CREATE TABLE IF NOT EXISTS {SchemaTestTable}
(
    {SchemaTestTable_schema}
)
USING DELTA
COMMENT "Contains schema testing data"
LOCATION "{SchemaTestTable_path}"
