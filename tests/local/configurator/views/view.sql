-- SPETLR.CONFIGURATOR key: MyViewId
CREATE OR REPLACE TEMPORARY VIEW IF NOT EXISTS SomeViewName
--   (today COMMENT 'right now') This construct is not currenly supported.
COMMENT 'Better than a clock'
TBLPROPERTIES ('m.prop'='hello')
AS SELECT current_date();

-- SPETLR.CONFIGURATOR key: MyArrayObject
-- SPETLR.CONFIGURATOR array:
-- SPETLR.CONFIGURATOR  - hello
-- SPETLR.CONFIGURATOR  - world
-- SPETLR.CONFIGURATOR object:
-- SPETLR.CONFIGURATOR   nested: foobar

;
