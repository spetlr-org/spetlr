# ETL Log Orchestrator and Log Transformers

## Introduction

This document outlines the usage of the `LogOrchestrator` and `LogTransformers` classes. These tools are designed to provide a plug-and-play solution for measuring and recording data metrics during an ETL process. This includes row counts, null count, and a wide variety of other customizable metrics. The user has the option to implement their own logging logic by extending the `LogTransformer` class and implementing the abstract methods. Before exploring `LogOrchestrator`, it is essential to understand the foundational SPETLR OETL framework, as `LogOrchestrator` extends this underlying logic.

## Log Orchestrator 

The `LogOrchestrator` functions similarly to the standard `Orchestrator`. It incorporates the same .step() method, which enables calls to `.extract_from()`, `.transform_with()`, and `load_into()`. However, `LogOrchestrator` introduces two key features:

1. `LogOrchestrator` requires a list of handles at instantiation. These handles specify the destination for the output of any logging operations.
2. `LogOrchestrator` introduces a new method, `.log_with()`. This method accepts a `LogTransformer` as input, which operates as a regular `Transformer` that returns a dataframe with recorded metrics.

On calling `.execute()`, the `LogOrchestrator` executes the ETL process, but also performs logging for any `LogTransformers` present in the `.log_with()` steps. The outputs from these `LogTransformers` are automatically loaded into the handles provided during the initial instantiation of the orchestrator.

### Example

```python
from spetlr.delta import DeltaHandle
from spetlr.etl.log import LogOrchestrator
from spetlr.etl.log.log_transformers import CountLogTransformer

etl = LogOrchestrator(
    handles=[
        DeltaHandle(name="db.MyLogTable"),
    ]
)

etl.extract_from(...)

etl.transform_with(...)

etl.load_into(...)

etl.log_with(
    CountLogTransformer(
        log_name="MyFirstLogOrchestrator",
        dataset_input_keys=["df_key_from_transformation_step"],
    )
)
```

In this example, in addition to standard orchestrator logic, the ETL flow uses a `CountLogTransformer` on the dataframe produced by the `.transform_with()` step. This logs the number of rows in the input dataframe. The results are then loaded into the handle `db.MyLogTable`.

### Important considerations

The `.log_with()` step can be included anywhere in the ETL flow. It only needs to reference a dataset key from a previous step.

The `.log_with()` step provides methods to process either a single dataframe or multiple dataframes, similar to the .`process()` and `.process_many()` methods in the standard Transformer class.

Multiple log steps can be added as required, irrespective of the number of input dataset keys or number of `.log_with()` steps. The ETL flow will perform a SINGLE write operation per destination handle.

## Log Transformer

The `LogTransformer` is a base class that can be inherited to implement custom logging logic. Various subclasses with predefined logging functionalities are available. These include common operations such as retrieving the number of rows or null values in a dataset.