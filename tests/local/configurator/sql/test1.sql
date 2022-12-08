-- atc.Configurator key: MySparkDb
CREATE DATABASE IF NOT EXISTS `my_db1{ID}`
COMMENT "Dummy Database 1"
LOCATION "/tmp/foo/bar/my_db1/";

-- atc.Configurator key: MyDetailsTable
CREATE TABLE IF NOT EXISTS `my_db1{ID}.details`
(
  {MyAlias_schema},
  another int
  -- comment with ;
)
USING DELTA
COMMENT "Dummy Database 1 details"
LOCATION "/{MNT}/foo/bar/my_db1/details/";

-- pure configurator magic in this statement
-- atc.Configurator key: MyAlias
-- atc.Configurator alias: MySqlTable
;


-- ATC.CONFIGURATOR key: MySqlTable
-- atc.Configurator delete_on_delta_schema_mismatch: true
CREATE TABLE IF NOT EXISTS `{MySparkDb}.tbl1`
(
  a int,
  b int,
  c string,
  d timestamp
)
USING DELTA
COMMENT "Dummy Database 1 table 1"
LOCATION "/{MNT}/foo/bar/my_db1/tbl1/";
