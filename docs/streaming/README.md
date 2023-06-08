# Streaming
*"You can use Databricks for near real-time data ingestion, processing, machine learning, and AI for streaming data."*

*"Apache Spark Structured Streaming is a near-real time processing engine that offers end-to-end fault tolerance with exactly-once processing guarantees using familiar Spark APIs. Structured Streaming lets you express computation on streaming data in the same way you express a batch computation on static data. The Structured Streaming engine performs the computation incrementally and continuously updates the result as streaming data arrives."*

[Source: Databricks.com](https://docs.databricks.com/structured-streaming/index.html)


## Delta streaming
Since Spetlr primarily is built as batch-processing ETL, structured streaming is available in the following way:

- The `DeltaHandle` class includes a method `.read_stream()` for reading a stream.
- However, because streaming behaves differently than batch processing, a stream requires both a read and a write. As a result, the delta-handle is unable to have a write stream method. To make streaming work for the Spetlr ETL framework, the `foreachbatch` stream method is utilized. The class `StreamLoader()` should be used in the ETL as the `load_into()` method. 

### Example: Delta streaming
```python
from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.etl.loaders import SimpleLoader
from spetlr.delta import DeltaHandle
from spetlr.configurator import Configurator

dh_source = DeltaHandle(...)
dh_target = DeltaHandle(...)

Orchestrator()\
    .extract_from(StreamExtractor(dh_source, dataset_key="MyTbl"))\
    .load_into(
    StreamLoader(
        loader=SimpleLoader(handle=dh_target, mode="append"),
        await_termination=True,
        checkpoint_path=Configurator().get("MyTblMirror", "checkpoint_path"),
        )
    )\
    .execute()

```
## Autoloader

The databricks autoloader can *"(...) incrementally and efficiently processes new data files as they arrive in cloud storage without any additional setup."* - [link](https://docs.databricks.com/ingestion/auto-loader/index.html). The autoloader is implemented as a handle class as follows:

- A handle class called `AutoloaderHandle`.
- `AutoloaderHandle` has a `.read_stream()` method.

The autoloader is combined with the `StreamLoader()` when used in the Spetlr ETL framework - [see previous section](#delta-streaming).

### Example: Streaming with autoload

```python
from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.autoloader import AutoloaderHandle
from spetlr.etl.loaders import SimpleLoader
from spetlr.delta import DeltaHandle
from spetlr.configurator import Configurator


dh_target = DeltaHandle(...)

Orchestrator()\
    .extract_from(
        StreamExtractor(
            AutoloaderHandle.from_tc("AvroSource"), dataset_key="AvroSource"
        )
    )\
    .load_into(
        StreamLoader(
            loader=SimpleLoader(handle=dh_target),
            await_termination=True,
            checkpoint_path=Configurator().get("AvroSink", "checkpoint_path"),
        )
    )\
    .execute()
```
