CREATE OR REPLACE TEMPORARY VIEW SimpleSqlTransformerTestResult AS
SELECT
    a,
    b,
    SecondTable.c
FROM FirstTable
LEFT JOIN SecondTable
    ON FirstTable.a = SecondTable.a
