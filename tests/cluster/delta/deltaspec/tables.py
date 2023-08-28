from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec

# Note that the table propery delta.columnMapping.maxColumnId is completely ignored

base = DeltaTableSpec.from_sql(
    """
    CREATE TABLE myDeltaTableSpecTestDb{ID}.tbl
    (
        c double,
        d string NOT NULL COMMENT "Whatsupp",
        onlyb int,
        a int,
        b string
    )
    USING DELTA
    LOCATION "/tmp/somewhere/over/the/rainbow"
    TBLPROPERTIES
    (
      "delta.columnMapping.maxColumnId" = "6"
    )
    """
)

target = DeltaTableSpec.from_sql(
    """
    CREATE TABLE myDeltaTableSpecTestDb{ID}.tbl
    (
        a int NOT NULL COMMENT "gains not null",
        b string,
        c double,
        d string,
        onlyt string COMMENT "Only in target"
    )
    USING DELTA
    COMMENT "Contains useful data"
    LOCATION "/tmp/somewhere/over/the/rainbow"
    """
)
