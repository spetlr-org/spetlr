CREATE TABLE IF NOT EXISTS "my_db1{ID}.tbl1"(
a int,
b int,
c string,
d timestamp
)
USING DELTA
COMMENT "Dummy Database 1 table 1"
LOCATION "/tmp/foo/bar/my_db1/tbl1/"
