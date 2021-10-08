# Functions documentation

Functions in atc-dataplatform:

* [Drop table cascade](#drop-table-cascade)

##Drop table cascade
The function drops a databricks database table and deletes the directory associated with the table.


```python
def drop_table_cascade(DBDotTableName: str) -> None:    
    ...
```
Usage example: 
``` python
drop_table_cascade("test.testTable")
```

### Example

This section elaborates on how the `drop_table_cascade` function works with a small example.

Create a test database:
```sql

CREATE DATABASE IF NOT EXISTS test
COMMENT "A test database"
LOCATION "/tmp/test/"

```
Create a test table:
```sql
CREATE TABLE IF NOT EXISTS test.testTable(
  Id STRING,
  sometext STRING,
  someinteger INT
)
USING DELTA
COMMENT "Contains test data"
LOCATION "/tmp/test/testTable"
```

Insert dummy data into the test table:
```sql
insert into test.testTable values ("ID1","hello",1)
```
Even if the table is dropped using `drop table test.testTable`, the table files are not neccesarily deleted - see [Databrick documentation](https://docs.databricks.com/sql/language-manual/sql-ref-syntax-ddl-drop-table.html). 
Try to run the following code and see by your self:

``` python
drop table test.testTable; 

CREATE TABLE IF NOT EXISTS test.testTable(
  Id STRING,
  sometext STRING,
  someinteger INT
)
USING DELTA
COMMENT "Contains test data"
LOCATION "/tmp/test/testTable";

select * from test.testTable;

```

You will notice, that when creating the table using files from the same path as previous, no files were deleted using `drop table`. 
If you in stead use:

``` python
drop_table_cascade("test.testTable")
```

The testTable is dropped and the files (directory) are deleted.

