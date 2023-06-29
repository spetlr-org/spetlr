CREATE DATABASE IF NOT EXISTS {SparkTestDbMismatch}
LOCATION "{SparkTestDbMismatch_path}";

CREATE TABLE IF NOT EXISTS {MismatchSparkTestTable}(
a int,
b int -- Added a new column
)
USING DELTA
LOCATION "{MismatchSparkTestTable_path}"
