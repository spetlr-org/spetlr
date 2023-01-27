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
