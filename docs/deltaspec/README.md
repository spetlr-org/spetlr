
# DeltaTableSpec

## Abstract

The `DeltaTableSpec` class contains all information about a delta table that can be 
given in a `CREATE TABLE` statement.

The class can be initialized in pure python, or by parsing a `CREATE TABLE` 
statement. In addition, the class is able to lift all necessary information from the 
spark catalog that fully describe the table. Using these two channels, 1. from code 
and 2. from disk, the class can make statements about the degree of agreement 
between the two. Crucially, the class can formulate the `ALTER TABLE` statements 
that are necessary to bring the table in spark into alignment with the specification 
from code. This is its primary function.

## Introduction

Taking a step back from the mechanisms of spark, one could argue that there are 
these competing statements that all describe a delta table to some degree:
- A `CREATE TABLE` statement
- A spark data frame (to be written to disk)
- A delta table on a storage media or in the spark catalog

In order to enable more dynamic analysis of their mutual (dis-)agreements, these 
have been extended with the `DeltaTableSpec` class which can exist:
- as python code: `DeltaTableSpec(name="...", schema=...)`
- as a class instance in memory

The class has methods that enable going back and forth between each of these forms:
- python code &harr; object instance: `__init__` and `repr(tbl)` are guaranteed to 
  be mutual inverses. The result of `eval(repr(tbl))` compares equal to the 
  original object.
- sql code &harr; object instance: 
  - `DeltaTableSpec.from_sql(str)` will create an instance from sql code
  - `tbl.get_create_sql()` will return a fully formed create statement, 
    guaranteed to be the inverse of the above.
- delta table &harr; object instance:
  - `DeltaTableSpec.from_path(str)` and `DeltaTableSpec.from_name(str)` will read 
    all table details from spark.
  - `tbl.make_storage_match()` will execute the necessary create sql statement to 
    make the result of the `from_name` call compare equal to the specification in `tbl`

## Reference
For a detailed reference, please see the docstrings of each method on the class.



