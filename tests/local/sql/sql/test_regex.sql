-- spetlr.Configurator key: TestDb
CREATE DATABASE IF NOT EXISTS my_test_db
COMMENT "Test Database"
LOCATION "/tmp/foo/bar/my_test_db/"
WITH DBPROPERTIES ("property_name"="property_value");

-- COMMAND ----------

-- spetlr.Configurator key: TestTable
CREATE TABLE IF NOT EXISTS my_test_db.test_table
(
  id INT,
  value STRING
  -- comment with a semicolon ;
)
USING DELTA
COMMENT "Test table with a location"
LOCATION "/mnt/foo/bar/my_test_db/test_table/";

-- spetlr.Configurator key: AnotherTable
CREATE TABLE IF NOT EXISTS my_test_db.another_table
(
  name STRING,
  age INT
)
USING DELTA
COMMENT "Another test table"
LOCATION "/mnt/foo/bar/my_test_db/another_table/";

-- This should be removed by regex
DROP TABLE my_test_db.dropped_table;

-- spetlr.Configurator key: PlaceholderExample
CREATE TABLE IF NOT EXISTS my_test_db.config_table
(
  config_name STRING,
  config_value STRING
)
USING DELTA
COMMENT "Config table"
LOCATION "/{MNT}/foo/bar/my_test_db/config_table/";
