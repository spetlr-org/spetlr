# Extractors documentation
This page documents all the spetlr extractors, following the OETL pattern. 

Extractors in spetlr:

* [Eventhub stream extractor](#eventhub-stream-extractor)
* [Incremental extractor](#incremental-extractor)


## Eventhub stream extractor
This extractor reads data from an Azure eventhub and returns a structural streaming dataframe.

Under the hood [spark azure eventhub](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md) is used, and this [maven library](https://mvnrepository.com/artifact/com.microsoft.azure/azure-eventhubs-spark)

```python
from spetlr.etl import Extractor

class EventhubStreamExtractor(Extractor):
    def __init__(self, 
                 consumerGroup: str,
                 connectionString: str = None,
                 namespace: str = None,
                 eventhub: str = None,
                 accessKeyName: str = None,
                 accessKey: str = None,
                 maxEventsPerTrigger: int = 10000):
        ...
```

Usage example with connection string:
``` python
eventhubStreamExtractor = EventhubStreamExtractor(
    consumerGroup="TestConsumerGroup",
    connectionString="TestSecretConnectionString",
    maxEventsPerTrigger = 100000
)
```

Usage example without connection string:
``` python
eventhubStreamExtractor = EventhubStreamExtractor(
    consumerGroup="TestConsumerGroup",
    namespace="TestNamespace",
    eventhub="TestEventhub",
    accessKeyName="TestAccessKeyName",
    accessKey="TestSecretAccessKey",
    maxEventsPerTrigger = 100000
)
```

Usage example with defining start timestamp:
``` python
eventhubStreamExtractor = EventhubStreamExtractor(
    consumerGroup="TestConsumerGroup",
    connectionString="TestSecretConnectionString",
    maxEventsPerTrigger = 100000,
    startEnqueuedTime = datetime.utcnow()
)
```

### Example

This section elaborates on how the `EventhubStreamExtractor` extractor works and how to use it in the OETL pattern. 

```python
from pyspark.sql import DataFrame
from pyspark.sql.types import T
import pyspark.sql.functions as F

from spetlr.etl import Transformer, Loader, Orchestrator
from spetlr.extractors import EventhubStreamExtractor
from spetlr.functions import init_dbutils

class BasicTransformer(Transformer):
    def process(self, df: DataFrame) -> DataFrame:
        print('Current DataFrame schema')
        df.printSchema()

        df = df.withColumn('body', F.col('body').cast(T.StringType()))

        print('New DataFrame schema')
        df.printSchema()
        return df


class NoopLoader(Loader):
    def save(self, df: DataFrame) -> DataFrame:
        df.write.format('noop').mode('overwrite').save()
        return df


print('ETL Orchestrator using EventhubStreamExtractor')
etl = (Orchestrator()
        .extract_from(EventhubStreamExtractor(
            consumerGroup="TestConsumerGroup",
            connectionString=init_dbutils().secrets.get(scope = "TestScope", key = "TestSecretConnectionString"),
            maxEventsPerTrigger = 100000
        ))
        .transform_with(BasicTransformer())
        .load_into(NoopLoader())
        )
result = etl.execute()
```


## Incremental extractor

This extractor only select the newest data from the source 
by comparing with a target table.

### Example

What is extracted?

```python
"""
Source has the following data:

|id| stringcol    | timecol          |
|--|--------------|------------------|
|1 | "string1"    | 01.01.2021 10:50 |
|22| "string2inc" | 01.01.2021 10:56 |
|3 | "string3"    | 01.01.2021 11:00 |

Target has the following data

|id| stringcol    | timecol          |
|--|--------------|------------------|
|1 | "string1"    | 01.01.2021 10:50 |
|2| "string2"     | 01.01.2021 10:55 |

So data from after 01.01.2021 10:55 should be read

|id| stringcol    | timecol          |
|--|--------------|------------------|
|22| "string2inc" | 01.01.2021 10:56 |
|3 | "string3"    | 01.01.2021 11:00 |
"""
```

How to use it:
```python
from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from spetlr.etl.extractors import IncrementalExtractor
from spetlr.delta import DeltaHandle
from spetlr.etl import Transformer, Loader, Orchestrator

class BasicTransformer(Transformer):
    def process(self, df: DataFrame) -> DataFrame:
        df = df.withColumn('idAsString', f.col('id').cast("string"))
        return df

class NoopLoader(Loader):
    def save(self, df: DataFrame) -> DataFrame:
        df.write.format('noop').mode('overwrite').save()
        return df

etl = (Orchestrator
        .extract_from(IncrementalExtractor(
            handle_source=DeltaHandle.from_tc("SourceId"),
            handle_target=DeltaHandle.from_tc("TargetId"),
            time_col_source="TimeColumn",
            time_col_target="TimeColumn",
            dataset_key="source"
        ))
        .transform_with(BasicTransformer())
        .load_into(NoopLoader())
        )
result = etl.execute()
```
