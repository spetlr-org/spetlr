# Streaming

# Example 1: streaming
```python
from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.tables import TableHandle
Orchestrator().extract_from(StreamExtractor(handle=myhandle)).load_into(
    StreamLoader(handle=TableHandle())
)
```


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
