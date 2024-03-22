CREATE DATABASE IF NOT EXISTS {SparkTestDbMismatch}
LOCATION "{SparkTestDbMismatch_path}";

CREATE TABLE IF NOT EXISTS {MismatchSparkTestTableToUpdate}(
a int
)
USING DELTA
LOCATION "{MismatchSparkTestTableToUpdate_path}"
