
# SPETLR testing

## Overview

SPETLR recommends to implement testing of your Databricks environment.

The testing can be devided into three areas:

* [Unit test](#unit-test)
* [Hybrid test](#hybrid-test)
* [Integration test](#integration-test)



## Unit test

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

The above example is just one way to do unit testing of a transformer. We also refer to [Python Unittest.mock](https://docs.python.org/3/library/unittest.mock.html)  for making unittests to ensure that logic in your ETL steps work reliable.

## Hybrid test

Here is an example of testing an orchestrator, but ingesting test data. This can be done without a cluser, if all handlers use test-handlers.

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

## Integration test

The [SPETLR configurator](/docs/configuration/README.md) enables integrations by making abstractions of the delta tables. 

To configure `init_configurator` and `debug_configurator` - see [SPETLR Configurator](/docs/configuration/README.md).

To configure a SparkSqlExecutor - see [SPETLR SqlExecutor](/docs/sql/README.md#sqlexecutor)

**Ensure that your databases has the `ID` variable configurated correctly.**


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
