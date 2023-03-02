The orchestrators package contains complete orchestration steps
that extract and transform from a set of sources and load the result to a target.

# EventHub to Delta
A very common pattern in data platforms is that json documents are published to an 
azure eventhub with capture enabled. The dataplatform wants to ingest that data into 
a delta table with a given schema before carrying out further transformations.

The class `EhJsonToDeltaOrchestrator` has been designed to carry out this task with 
minimal configuration required.

The arguments to this orchestrator consist of
- an `EventHubCaptureExtractor` or the key to a TableConfigurator item from which it 
  can be initialized
- a `DeltaHandle`

All important configurations follow from the schema and partitioning of the delta table.
- The delta table must use one of the following partitioning sets
  - either "y,m,d" or "y,m,d,h" whichever is used in the eventhub capture
  - or "pdate" which is the timestamp, constructed from "ymd" or "ymdh"
  The capture files will be read from the latest partition in delta and forward, 
    only. (The latest partition will be truncated and re-read to ensure complete but 
    non-overlapping reads.) This incremental approach ensures efficiency.
- The delta table _may_ have columns like EnqueuedTimeUtc, which can be taken directly 
  from the EventHub capture avro files.
- Any additional columns that are not already covered by the above, are collected, 
  and the body payload of the eventhub is extracted as a json document from which 
  these columns are then extracted.

There may be cases where only a subset of rows is desired to be extracted in the 
process. Here the orchestrator offers the method `.filter_with` which allows 
additional transformation steps to be injected before the rows are finally appended 
to the delta table.

# Eventhub to medallion architecture

*"A medallion architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture:* 

*(from Bronze ⇒ Silver ⇒ Gold layer tables).*

*Medallion architectures are sometimes also referred to as "multi-hop" architectures."*

[Source: Databricks.com](https://www.databricks.com/glossary/medallion-architecture)

## EventHub to Bronze 

The class `EhToDeltaBronzeOrchestrator` has been designed to carry out the task of ingest eventhub data to a bronze layer. Data is always _appended_ to the bronze table. By utilizing `EhJsonToDeltaExtractor` from [previous section](#-EventHub-to-Delta) data is extracting incrementally.

*"The Bronze layer is where we land all the data from external source systems. The table structures in this layer correspond to the source system table structures "as-is," along with any additional metadata columns that capture the load date/time, process ID, etc. The focus in this layer is quick Change Data Capture and the ability to provide an historical archive of source (cold storage), data lineage, auditability, reprocessing if needed without rereading the data from the source system."*

[Source: Databricks.com](https://www.databricks.com/glossary/medallion-architecture)

Using the orchestrator without adding any filtering `.filter_with` the output schema is the following:

| **Column Name**   | **Data type** | **Explanation**                                                                                                    |
|-------------------|---------------|--------------------------------------------------------------------------------------------------------------------|
| BodyId            | Long          | A ID generated to give a unique id for each row in the bronze table.  Calculated based on sha2 hashing the *Body*. |
| Body              | String        | The eventhub body casted as a string - for readability and searchability.                                          |
| EnqueuedTimestamp | Timestamp     | The enqueueded time of the eventhub row.                                                                           |
| StreamingTime     | Timestamp     | A timestamp added in the moment the orchestrator processed eventhub data.                                          |
| pdate             | Timestamp     | A transformation of the eventhub partitioning set to a timestamp. See [previous section](#-EventHub-to-Delta)                                                 |



### Example

The arguments to this orchestrator consist of
- The source handle: an `EventHubCaptureExtractor` or the key to a TableConfigurator item from which it can be initialized
- The target handle: a `DeltaHandle`

There may be cases where only a subset of rows is desired to be extracted in the 
process. Here the orchestrator offers the method `.filter_with` which allows 
additional transformation steps to be injected before the rows are finally appended to the delta table.


```python
from atc.delta import DeltaHandle
from atc.eh.EventHubCaptureExtractor import EventHubCaptureExtractor
from atc.orchestrators import EhToDeltaBronzeOrchestrator

eh=EventHubCaptureExtractor.from_tc("eh_id")
dh=DeltaHandle.from_tc("dh_id")

orchestrator = EhToDeltaBronzeOrchestrator(eh=eh, dh=dh)
orchestrator.execute()

```


## Eventhub to Silver

The class `EhToDeltaSilverOrchestrator` has been designed to carry out the task of unpacking and transforming bronze eventhub data to the silver layer.

*"In the Silver layer of the lakehouse, the data from the Bronze layer is matched, merged, conformed and cleansed ("just-enough") so that the Silver layer can provide an "Enterprise view" of all its key business entities, concepts and transactions. (e.g. master customers, stores, non-duplicated transactions and cross-reference tables)."* 

[Source: Databricks.com](https://www.databricks.com/glossary/medallion-architecture)

Data is per default _incrementally upserted_ to the silver table. By utilizing `EhJsonToDeltaTransformer` from [previous section](#-EventHub-to-Delta) the schema of the `dh_target` is used for unpacking the eventhub bronze schema. It is therefore only neccesary to define the target delta schema. There may be cases where only a subset of rows is desired to be extracted in the 
process. Here the orchestrator offers the method `.filter_with` which allows 
additional transformation steps to be injected before the rows are finally upserted to the delta table.

**Note:** It is possible to choose either _append_ or _overwrite_ instead of upsert. Keep in mind, that the extracter will in these cases use the `SimpleExtractor` and `SimpleLoader` for extracting/loading data.

### Example

The arguments to this orchestrator consist of
- The source handle: a `DeltaHandle` (the bronze eventhub table)
- The target handle: a `DeltaHandle` (the silver eventhub table)

*The schema of `dh_target` defines how the eventhub data is unpacked.*

```python
from atc.delta import DeltaHandle
from atc.orchestrators import EhToDeltaSilverOrchestrator

dh_source=DeltaHandle.from_tc("dh_source_id")
dh_target=DeltaHandle.from_tc("dh_target_id")

orchestrator = EhToDeltaSilverOrchestrator(dh_source=dh_source, dh_target=dh_target)

orchestrator.execute()

```

## Eventhub to gold

What about the gold layer? Since the gold layer often associates with customade business logic - no orchestrator is implemented for the purpose.

*"Data in the Gold layer of the lakehouse is typically organized in consumption-ready "project-specific" databases. The Gold layer is for reporting and uses more de-normalized and read-optimized data models with fewer joins. The final layer of data transformations and data quality rules are applied here."*

[Source: Databricks.com](https://www.databricks.com/glossary/medallion-architecture)
