-- spetlr.Configurator key: MySparkDb
CREATE DATABASE IF NOT EXISTS my_db1
COMMENT "Dummy Database 1"
LOCATION "/tmp/foo/bar/my_db1/"
WITH DBPROPERTIES ("property_name"="property_value")

-- COMMAND ----------

-- spetlr.Configurator key: MyDetailsTable
CREATE TABLE IF NOT EXISTS my_db1.details
(
  another int
  -- comment with ;
)
USING DELTA
COMMENT "Dummy Database 1 details"
LOCATION "/mnt/foo/bar/my_db1/details/";

-- pure configurator magic in this statement
-- spetlr.Configurator key: MyAlias
-- spetlr.Configurator alias: MySqlTable
;


-- ATC.CONFIGURATOR key: MySqlTable
-- spetlr.Configurator delete_on_delta_schema_mismatch: true
CREATE TABLE IF NOT EXISTS my_db1.tbl1
(
  a int,
  b int,
  c string,
  d timestamp
)
USING DELTA
OPTIONS (key1='val1', key2="val2")
PARTITIONED BY ( a, b )
CLUSTERED BY ( c,d )
         SORTED BY ( a, b DESC )
        INTO 5 BUCKETS
COMMENT "Dummy Database 1 table 1"
LOCATION "/{MNT}/foo/bar/my_db1/tbl1/"
TBLPROPERTIES ( key1='val1', key2='val2' )
;
