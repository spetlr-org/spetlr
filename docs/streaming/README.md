# Streaming
*"You can use Databricks for near real-time data ingestion, processing, machine learning, and AI for streaming data."*

*"Apache Spark Structured Streaming is a near-real time processing engine that offers end-to-end fault tolerance with exactly-once processing guarantees using familiar Spark APIs. Structured Streaming lets you express computation on streaming data in the same way you express a batch computation on static data. The Structured Streaming engine performs the computation incrementally and continuously updates the result as streaming data arrives."*

[Source: Databricks.com](https://docs.databricks.com/structured-streaming/index.html)


## Delta streaming
Since Spetlr primarily is built as batch-processing ETL, structured streaming available in the following way:

- The `DeltaHandle` class has a method called `.read_stream()` which is used for reading a stream.
- Since streaming behave different that batch process, a stream needs both a read and a write. This means that the delta-handle is unable to have a write-stream - just to emphasize - the DeltaHandle does NOT have a stream write function. In order to make streaming work for the Spetlr ETL framework the `foreachbatch` stream method is applied. The class `StreamLoader()` should be used in the ETL as the `load_into()` method. 

### Example: Delta streaming
```python
from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.tables import TableHandle
Orchestrator().extract_from(StreamExtractor(handle=myhandle)).load_into(
    StreamLoader(handle=TableHandle())
)

Orchestrator()
    .extract_from(StreamExtractor(dh, dataset_key="MyTbl"))
    .load_into(
    StreamLoader(
        handle=dh_target,
        options_dict={},
        format="delta",
        await_termination=True,
        mode="append",
        checkpoint_path=Configurator().get("MyTblMirror", "checkpoint_path"),
        )
    )
    .execute()

```
## Autoloader

The databricks autoloader can *"(...) incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup."* - [link](https://docs.databricks.com/ingestion/auto-loader/index.html). The autoloader is implemented as a handle class as follows:

- A handle class called `AutoloaderHandle`
- `AutoloaderHandle` has a `.read_stream()` method

The autoloader is combined with the `StreamLoader()` when used in the Spetlr ETL framework - [see previous section](#delta-streaming).

### Example: Streaming with autoload

```python
from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.tables import TableHandle
from spetlr.autoloader.AutoloaderHandle

 o = Orchestrator()
    .extract_from
    (
        StreamExtractor(
            AutoloaderHandle.from_tc("AvroSource"), dataset_key="AvroSource"
        )
    )
    .load_into
    (
        StreamLoader(
            handle=dh_sink,
            options_dict={},
            format="delta",
            await_termination=True,
            mode="append",
            checkpoint_path=tc.get("AvroSink", "checkpoint_path"),
        )
    )
    .execute()
```
