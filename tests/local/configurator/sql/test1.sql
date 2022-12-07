-- atc.Configurator tag: MySparkDb
CREATE DATABASE IF NOT EXISTS `my_db1{ID}`
COMMENT "Dummy Database 1"
LOCATION "/tmp/foo/bar/my_db1/";

-- atc.Configurator tag: MyDetailsTable
CREATE TABLE IF NOT EXISTS `my_db1{ID}.details`
(
  {MySqlTable_schema},
  another int
  -- comment with ;
)
USING DELTA
COMMENT "Dummy Database 1 details"
LOCATION "/{MNT}/foo/bar/my_db1/details/";


-- atc.Configurator tag: MySqlTable
-- atc.Configurator delete_on_delta_schema_mismatch: true
CREATE TABLE IF NOT EXISTS `my_db1{ID}.tbl1`
(
  a int,
  b int,
  c string,
  d timestamp
)
USING DELTA
COMMENT "Dummy Database 1 table 1"
LOCATION "/{MNT}/foo/bar/my_db1/tbl1/";
