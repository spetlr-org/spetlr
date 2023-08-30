
# SPETLR testing

## Overview

SPETLR recommends to implementing testing of your Databricks environment.

The testing can be divided into two areas:

* [Local test](#local-test)
* [Cluster test](#cluster-test)

Furthermore, SPETLR provides an inspiration on how to structure tests:
* [Folder structure](#folder-structure)

## Local test


Local test can be:
* Test of transformers
* Test of functions
* Test of ETL using TestHandle

### Local Unit Test

Here is an example of how to do unit-testing on a transformer. This can be done without a cluster. 
```python
import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.spark import Spark


class ExampleTransformerTest(DataframeTestCase):
    def test_transformer(self):
        inputSchema = T.StructType(
            [
                T.StructField("Col1", T.StringType(), True),
                T.StructField("Col2", T.IntegerType(), True),
            ]
        )
        inputData = [("Col1Data", 42)]

        input_df = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        expectedData = [(42, 13.37, "Col4Data")]

        transformed_df = YourTransformer().process(input_df)

        self.assertDataframeMatches(transformed_df, None, expectedData)

```

The above example is just one way to do unit testing of a transformer. We also refer to [Python Unittest.mock](https://docs.python.org/3/library/unittest.mock.html)  for making unittests to ensure that logic in your ETL steps work reliably.

### Local ETL test

Here is an example of testing an orchestrator, but ingesting test data. This can be done without a cluster if all handlers use test-handlers.

```python
from spetlrtools.testing.TestHandle import TestHandle
from spetlr.utils import DataframeCreator

test_schema =  ...
test_columns = ...
test_data = ...

expected_schema =  ...
expected_columns = ...
expected_data = ...

params = BusinessETLParameters()

params.source_dh =  TestHandle(
            provides=DataframeCreator.make_partial(
                test_schema , 
                test_columns,
                 test_data 
            )
        )

params.target_dh =  TestHandle(
            provides=DataframeCreator.make_partial(
                expected_schema , 
                expected_columns,
                [] 
            )
        )

orchestrator= BusinessETLOrchestrator(params)

orchestrator.execute()

self.assertDataframeMatches(params.target.overwritten, None, expected_data )
```

## Cluster test

Cluster tests can be:
* Tests of ETL using SPETLR configurator (table abstractions / debug tables)
* Tests which uses dbutils
* Tests which uses the Apache Spark SQL Engine (and therefore cannot be tested on a local machine)
* Test that source and sink systems are accessible
    * Data lake paths
    * Evenhub connections
    * Sqlserver/database access


### Cluster ETL Integration test

The [SPETLR configurator](/docs/configuration/README.md) enables integrations by making abstractions of the delta tables. 

To configure `init_configurator` and `debug_configurator` - see [SPETLR Configurator](/docs/configuration/README.md).

To configure a SparkSqlExecutor - see [SPETLR SqlExecutor](/docs/sql/README.md#sqlexecutor)

**Ensure that your databases has the `ID` variable configured correctly.**


```python
import unittest
from test.environment import debug_configurator

# Your implemetation of the SparkSqlExecutor
from dataplatform.environment.SparkSqlExecutor import SparkSqlExecutor


class ExampleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        debug_configurator()

    ....

    def test_01_setup(self):
        SparkSqlExecutor().execute_sql_file("path-to-sql-file")
    def test_02_simple_integration_test(self):
        # Using SPETLR handlers in the ETL
        # Ensures that the ETL uses the
        # debug tables
        BusinessETLOrchestrator().execute()

```

All databases and tables are now created as debugging tables:

In the example above, the database `some_db` (production) will be created as debugging database like `some_db_14bc31f219584add9186bdd3f869d8eb` (debugging).

## Folder structure

Two main folder: *local* and *cluster*.

Folders underneath are the medallion arhcitecture. Example: *Bronze*. 

For each medal, a folder is created for each business specific area. Example: *BusinessArea1*

Prefix the filename into one of the following:

Local:
* test_unit_: If it is a unit test
* test_etl_: if it is a execution test of the orchestrator with TestHandles

Cluster:
* test_int_: If it is an intgration test that requires actual system integrations and a cluster to run.

```
.  
├── local  
│   ├── bronze  
│   │   └── BusinessArea1  
│   │       ├── test_unit_BA1_transformer.py  
│   │       └── test_etl_BA1.py  
│   ├── silver  
│   │   └── BusinessArea2  
│   │       └── test_unit_transformer.py  
│   └── utilities  
│       └── test_unit_md5_function.py  
└── cluster  
    ├── BusinessArea1  
    │   └── test_int_BA1.py  
    └── utilities  
        └── test_int_can_run.py  

```
This folder structure allows for a clear separation between local and cluster tests, making it easier to navigate and maintain your test suite.