# KafkaEventhubHandle
The `KafkaEventhubHandle` class provides convenient methods for interacting with Azure Event Hubs using the Kafka-compatible endpoint in Apache Spark applications. It allows for reading from and writing to Event Hubs via the Kafka format.

## Usage

### Reading from Event Hubs (via Kafka)

To read data from an Event Hub, use the `read` method:

```python
from spetlr.eh import EventhubHandle

# Initialize KafkaEventhubHandle instance
eh = EventhubHandle(
    consumer_group="consumer_group_name",
    namespace="your_namespace",
    eventhub="your_eventhub_name",
    accessKeyName="your_key_name",
    accessKey="your_key",
)

# Read data from Event Hub
df = eh.read()
```

### Writing to Event Hubs
To write data to an Event Hub, use the `append` method:

```python
from spetlr.eh import EventhubHandle

# Initialize KafkaEventhubHandle instance
eh = EventhubHandle(
    consumer_group="consumer_group_name",
    namespace="your_namespace",
    eventhub="your_eventhub_name",
    accessKeyName="your_key_name",
    accessKey="your_key",
)

# Assuming 'df' is your DataFrame containing data to be written
eh.append(df)
```

### Streaming with KafkaEventhubHandle

You can use `KafkaEventhubHandle` in streaming operations. Here's an example of streaming extraction and loading using `Orchestrator`, `StreamExtractor`, and `StreamLoader`:

```python
from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.etl.loaders import SimpleLoader
from spetlr.delta import DeltaHandle
from spetlr.configurator import Configurator
from spetlr.eh import EventhubHandle

# Initialize KafkaEventhubHandle instance for source
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

This pipeline extracts data from the Event Hub using `StreamExtractor` and `KafkaEventhubHandle`, loads it into the specified Delta table using `StreamLoader`, and executes the orchestration.

### Other Methods
* `from_tc`: Instantiates the handle using parameters defined in the configuration (via `Configurator`).
* `set_options_dict`: Sets Kafka options for the Event Hub connection.
* `get_options_dict`: Retrieves the Kafka options dictionary.
* `get_schema`: Retrieves the schema associated with the Event Hub data.
* `set_schema`: Sets the schema for the Event Hub data.
