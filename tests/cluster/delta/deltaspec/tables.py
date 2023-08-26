from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec

base = DeltaTableSpec.from_sql(
    """
    CREATE TABLE myDeltaTableSpecTestDb{ID}.table
    (

        c double,
        d string NOT NULL COMMENT "Whatsupp",
        onlyb int,
        a int,
        b string,
    )
    USING DELTA
    LOCATION "/somewhere/over/the/rainbow"
    """
)

target = DeltaTableSpec.from_sql(
    """
    CREATE TABLE myDeltaTableSpecTestDb{ID}.table
    (
        a int NOT NULL COMMENT "gains not null",
        b string,
        c double,
        d string,
        onlyt string COMMENT "Only in target",
    )
    USING DELTA
    COMMENT "Contains useful data"
    LOCATION "/somewhere/over/the/rainbow"
    """
)
