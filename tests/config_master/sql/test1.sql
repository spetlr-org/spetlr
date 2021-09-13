CREATE OR REPLACE TEMPORARY VIEW something AS
SELECT
    A.a,
    B.c
FROM foobar A

LEFT JOIN {table_id1} B
    ON A.a = B.a
