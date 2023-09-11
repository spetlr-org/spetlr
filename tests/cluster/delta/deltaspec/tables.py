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
        b int
    )
    USING DELTA
    LOCATION "/tmp/somewhere{ID}/over/the/rainbow"
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
    LOCATION "/tmp/somewhere{ID}/over/the/rainbow"
    TBLPROPERTIES (
      "my.cool.peoperty" = "bacon"
    )
    """
)

oldname = DeltaTableSpec.from_sql(
    """
    CREATE TABLE myDeltaTableSpecTestDb{ID}.namechange_old
    (
        b string,
        c double,
        d string
    )
    USING DELTA
    COMMENT "Contains useful data"
    LOCATION "/tmp/somewhere{ID}/namechange"
    """
)

newname = DeltaTableSpec.from_sql(
    """
    CREATE TABLE myDeltaTableSpecTestDb{ID}.namechange_new
    (
        b string,
        c double,
        d string
    )
    USING DELTA
    COMMENT "Contains useful data"
    LOCATION "/tmp/somewhere{ID}/namechange"
    """
)

oldlocation = DeltaTableSpec.from_sql(
    """
    CREATE TABLE myDeltaTableSpecTestDb{ID}.locchange
    (
        b string,
        c double,
        d string
    )
    USING DELTA
    COMMENT "Contains useful data"
    LOCATION "/tmp/somewhere{ID}/locchange/old"
    """
)

newlocation = DeltaTableSpec.from_sql(
    """
    CREATE TABLE myDeltaTableSpecTestDb{ID}.locchange
    (
        b string,
        c double,
        d string
    )
    USING DELTA
    COMMENT "Contains useful data"
    LOCATION "/tmp/somewhere{ID}/locchange/new"
    """
)
