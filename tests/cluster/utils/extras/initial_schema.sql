CREATE DATABASE IF NOT EXISTS {SparkTestDbMismatch}
LOCATION "{SparkTestDbMismatch_path}";

CREATE TABLE IF NOT EXISTS {MismatchSparkTestTable}(
a int
)
USING DELTA
LOCATION "{MismatchSparkTestTable_path}"
