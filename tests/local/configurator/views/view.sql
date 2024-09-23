-- SPETLR.CONFIGURATOR key: MyViewId
CREATE OR REPLACE TEMPORARY VIEW IF NOT EXISTS SomeViewName
--   (today COMMENT 'right now') This construct is not currenly supported.
COMMENT 'Better than a clock'
TBLPROPERTIES ('m.prop'='hello')
AS SELECT current_date();
