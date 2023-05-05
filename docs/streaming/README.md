# Streaming


## Delta streaming
Streaming in Spetlr is available in the following way:

- The `DeltaHandle` class has a method called `.read_stream()` which is used for reading a stream.
- Since streaming behave different that batch process, a stream needs both a read and a write. This means that the delta-handle is unable to have a write-stream - just to emphasize - the DeltaHandle does NOT have a stream write function. In order to make streaming work for the Spetlr ETL framework the `foreachbatch` stream method is applied. The class `StreamLoader()` should be used in the ETL as the `load_into()` method. 

# Example 1: streaming
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

# Example 2: streaming with autoload

```python
from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.tables import TableHandle
from spetlr.autoloader.AutoloaderHandle
Orchestrator().extract_from(StreamExtractor(handle=AutoloaderHandle())).load_into(
    StreamLoader(handle=TableHandle())
)
```
