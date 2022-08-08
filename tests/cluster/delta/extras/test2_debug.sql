CREATE DATABASE IF NOT EXISTS {SparkTestDb2}
LOCATION "{SparkTestDb2_path}";

CREATE TABLE IF NOT EXISTS {SparkTestTable2}(
a int
)
USING DELTA
LOCATION "{SparkTestTable2_path}"
