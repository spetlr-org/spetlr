CREATE DATABASE IF NOT EXISTS {SparkTestDb}
LOCATION "{SparkTestDb_path}";

CREATE TABLE IF NOT EXISTS {SparkTestTable1}(
a int,
b int -- Added a new column
)
USING DELTA
LOCATION "{SparkTestTable1_path}"
