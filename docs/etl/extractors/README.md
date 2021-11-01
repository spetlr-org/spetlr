# Extractors documentation
This page documents all the atc extractors, following the OETL pattern. 

Extractors in atc-dataplatform:

* [Eventhub stream extractor](#eventhub-stream-extractor)

## Eventhub stream extractor
This extractor reads data from an Azure eventhub and returns a data stream.

```python
class EventhubStreamExtractor(Extractor):
    def __init__(self, consumerGroup: str, connectionString: str = None, namespace: str = None, eventhub: str = None, accessKeyName: str = None, accessKey: str = None, maxEventsPerTrigger: int = 10000):
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

### Example

This section elaborates on how the `EventhubStreamExtractor` extractor works and to use it in the OETL pattern. 

```python
from pyspark.sql import DataFrame
from pyspark.sql.types import T
import pyspark.sql.functions as F

from atc.etl import Transformer, Loader, Orchestration
from atc.etl.extractos import EventhubStreamExtractor
from atc.spark import Spark

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


print('ETL Orchestrator using a single simple transformer')
eventhubStreamExtractor = EventhubStreamExtractor(
    consumerGroup="TestConsumerGroup",
    connectionString=dbutils.secrets.get(scope = "TestScope", key = "TestSecretConnectionString"),
    maxEventsPerTrigger = 100000
)

etl = (Orchestration
       .extract_from(eventhubStreamExtractor)
       .transform_with(BasicTransformer())
       .load_into(NoopLoader())
       .build())
result = etl.execute()
result.printSchema()
result.show()
```