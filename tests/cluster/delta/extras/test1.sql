CREATE DATABASE IF NOT EXISTS {SparkTestDb}
LOCATION "{SparkTestDb_path}";

CREATE TABLE IF NOT EXISTS {SparkTestTable1}(
a int
)
USING DELTA
LOCATION "{SparkTestTable1_path}"
