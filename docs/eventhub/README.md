
# EventHubHandle
The `EventHubHandle` class provides convenient methods for interacting with Azure Event Hubs in Apache Spark applications. It allows for reading data from and writing data to Event Hubs.

## Usage

### Reading from Event Hubs

To read data from an Event Hub, use the `read` method:

```python
from spetlr.eh import EventhubHandle

# Initialize EventHubHandle instance
eh = EventhubHandle(consumer_group="consumer_group_name", connection_str="your_connection_string")

# Read data from Event Hub
df = eh.read()
```

### Writing to Event Hubs
To write data to an Event Hub, use the append method:


```python
from spetlr.eh import EventhubHandle

# Initialize EventHubHandle instance
eh = EventhubHandle(consumer_group="consumer_group_name", connection_str="your_connection_string")

# Assuming 'df' is your DataFrame containing data to be written
eh.append(df)
```

### Streaming with Event Hub Handle

You can use EventHubHandle in streaming operations. Here's an example of streaming extraction and loading using `Orchestrator`, `StreamExtractor`, and `StreamLoader`:

Use EventHub Handle in streaming:
```python

from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.etl.loaders import SimpleLoader
from spetlr.delta import DeltaHandle
from spetlr.configurator import Configurator
from spetlr.eh import EventhubHandle

# Initialize EventHubHandle instance for source
eh_source = EventhubHandle(...)

# Initialize DeltaHandle instance for target
dh_target = DeltaHandle(...)

# Build and execute the orchestration pipeline
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

This pipeline extracts data from the Event Hub using `StreamExtractor` combined with the `EventhubHandle`, loads it into the specified target using `StreamLoader`, and executes the orchestration process.

### Other Methods
* `set_options_dict`: Sets options for the Event Hub connection.
* `get_options_dict`: Retrieves the options dictionary for the Event Hub connection.
* `get_schema`: Retrieves the schema associated with the Event Hub data.
* `set_schema`: Sets the schema for the Event Hub data.