CREATE DATABASE IF NOT EXISTS {SchemaTestDb}
COMMENT "Contains schema testing data";

CREATE TABLE IF NOT EXISTS {SchemaTestTable1}
(
    {SchemaTestTable1_schema}
)
USING {SchemaTestTable1_format}
COMMENT "Contains schema testing data";
