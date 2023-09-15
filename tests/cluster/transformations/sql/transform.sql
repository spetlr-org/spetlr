CREATE OR REPLACE TEMPORARY VIEW SimpleSqlTransformerTestResult AS
SELECT
    FirstTable.a,
    FirstTable.b,
    SecondTable.c
FROM FirstTable
LEFT JOIN SecondTable
    ON FirstTable.a = SecondTable.a
