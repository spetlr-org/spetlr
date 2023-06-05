# Utilities documentation

Utilities in spetlr:

* [Api Auto Config](#api-auto-config)
* [Test Utilities](#test-utilities)
* [Git Hooks](#git-hooks)
* [Cleanup Test Tables](#cleanup-test-tables)

## Api Auto Config

Using the method `spetlr.db_auto.getDbApi()` gives access to a 
`DatabricksAPI` instance that has been pre-configured for the 
current databricks instance. See [databricks-api](https://pypi.org/project/databricks-api/)
for usage documentation.

Under the hood the function uses the job context to get the host and token
when on the cluster. When using `spetlr` with databricks-connect, the `databricks-cli` is
called to configure the client. Thus, the function works without further configuration
in all contexts.

## Test Utilities

### DataframeCreator

The `DataframeCreator` is a helper class to assist in writing concise unittests.

Unittests typically take a dataframe, often created with `spark.createDataFrame` and transform it.
The function `createDataFrame` requires all data fields to be assigned a value, even if the given unittest is only concerned with a small subset of them.

This class allows the user to specify which columns she wants to give values for. All other columns will be assigned *null* values.

#### Usage:

```python3
from spetlr.utils import DataframeCreator
from spetlr.schema_manager.schema import get_schema

df = DataframeCreator.make_partial(
    schema=get_schema("""
                Id INTEGER,
                measured DOUBLE,
                customer STRUCT<
                    name:STRING,
                    address:STRING
                >,
                product_nos ARRAY<STRUCT<
                    no:INTEGER,
                    name:STRING
                >>
            """),
    columns=[
        "Id",
        # of the customer structure, only specify the name
        ("customer", ["name"]),
        # of the products array of structures, only specify the 'no' field in each row
        ("product_nos", ["no"])
    ],
    data=[
        (1, ("otto",), [(1,), (2,)]),
        (2, ("max",), []),
    ],
)
df.show()
```
Result:
```
| Id|measured|    customer|         product_nos|
+---+--------+------------+--------------------+
|  1|    null|{otto, null}|[{1, null}, {2, n...|
|  2|    null| {max, null}|                  []|
+---+--------+------------+--------------------+
```

## Git Hooks

A set of standard git hooks are included to provide useful functionality

- *pre-commit* before every commit, all files ending in `.py` will be formatted with the black code formatter

To use the hooks, they can be installed in any repository by executing this command from a path inside the repository:

    spetlr-git-hooks

To uninstall the hooks, simply run this command

    spetlr-git-hooks uninstall

## Cleanup Test Tables
When using the SPETLR Configurator to create abstraction of tables (test/debug tables),
it becomes handy to have an easy way of removing the test tables.

This can be achieved in the following ways:


### Delta Databases (and their tables)

```python
from spetlrtools.testing import DataframeTestCase
from spetlr.utils import CleanupTestDatabases

class ExampleTests(DataframeTestCase):
    
    @classmethod
    def tearDownClass(cls) -> None:
        CleanupTestDatabases()
```
