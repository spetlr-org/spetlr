CREATE DATABASE IF NOT EXISTS {SparkTestDbMismatch};

CREATE TABLE IF NOT EXISTS {MismatchSparkTestTable}(
a int,
b int -- Added a new column
)
