
# EventHubHandle

The `Configurator` contains logic to centralize and reuse configurations regarding eventhubs.
To make full use of this functionality when reading and writing 
 to eventhubs, a convenience class, `EventHubHandle`, have 
been provided. Use the classes like this

```python
from spetlr import Configurator
from spetlr.eh import EventhubHandle

tc = Configurator()
tc.add_resource_path('/my/config/files')

# 
eh = EventhubHandle.from_tc('MyEventhubId')


# To read from an eventhub_
df = eh.read()

# To write to the eventhub
eh.append(df)
```

Use EventHub Handle in streaming:
```python

from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.etl.loaders import SimpleLoader
from spetlr.delta import DeltaHandle
from spetlr.configurator import Configurator
from spetlr.eh import EventhubHandle

eh_source = EventhubHandle(...)
dh_target = DeltaHandle(...)

(
    Orchestrator()
        .extract_from(
            StreamExtractor(
                eh_source,
                dataset_key="MyTbl",
            )
        )
        .load_into(
            StreamLoader(
                loader=SimpleLoader(handle=dh_target, mode="append"),
                await_termination=True,
                checkpoint_path=Configurator().get("MyTblMirror", "checkpoint_path"),
            )
        )
        .execute()
)



```
