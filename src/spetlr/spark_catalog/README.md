# Spark Catalog API for table creation
# WORK IN PROGRESS
This folder contains an example of table creation using PySpark Catalog API.
It is currently very much a work-in-progress and contains both library code as well as example usage code.

## Inspiration from `sqlalchemy` & Entity Framework from C#

We draw heavy inspiration from Object Relational Mapping `sqlalchemy` in Python and Entity Framework from C#.
However, contrary to the above framework, the intention is only to centralize the definition, but still
use `pyspark.sql.DataFrame` for actual (row) instances.

## Why are we introducing a new method

The idea is to depart from defining Spark Tables using `.sql`-files and instead define these as classes.
This has pros and cons but the primary idea and concern is explained by the scope

## Scope

This section defines the usage scope of the current classes.

### Inside scope

* **Easier refactoring and clearer dependencies**: No implicit dependencies through column names.
* **Single-source-of-truth**: Keeping a single-source-of-truth for databases, tables and column definitions.
* **Enable IntelliSense**: The classes & models should enable IntelliSense-completion of attributes.
* **Slowly-changing-dimensions**: Implementation
  of [Slowly changing dimensions](https://en.wikipedia.org/wiki/Slowly_changing_dimension) as an mixin/add-on for
  tables.
* **Standardized merge-into-table**: Standardized merge operation with the table class implementation.
* **Class inheritance**: Enables inheritance of multiple columns that are standard to the super-class.
* **Other things I forgot**: Feel free to add.

### Outside scope

Currently, permission control is not initially given much in this framework, as the author (Thorbjørn) has limited
experience with this. However it is entirely possible that this is straightforward extension.

* The current implementation is restricted to `pyspark.sql.Catalog.createTable` with `source='delta'`.

## Pros & Cons

The primary concern is sharing these classes -- if the same class is implemented in different repositories, which
repository owns the model, and is allowed to change it?
Once in production, capturing changes on an existing tables is not a straightforward task, and requires a database
migration.
Therefore, these classes should mainly be viewed as a development/deployment enhancement, and will have limited, if any,
effect in
production. In production, it is likely that `.sql`-files with migration pattern will still reign supreme.

## Comparison of usage pattern compared to `.sql`-files

# Catalog, schema and table creation

Each catalog may contain several schemas (databases in SQL)

## Examples and usage patterns

1. Example of table creation using the interface `DeltaModel`

```python
from mlops_motor.src.models.environment.spark_api.models import DeltaModel


class MyTable(DeltaModel):
    def __init__(self):
        self.
```

2. Catalog, schema/database and table creation to be used in an Orchestrator:

```python
from mlops_motor.src.models.environment.spark_api.models import DeltaDatabase, DeltaCatalog


class NewTestDatabase(DeltaDatabase):
    def __init__(self):
        self.models: list[DeltaModel] = [
            ScfRaw(),  # instance of DeltaModel
            ScfBase()  # instance of DeltaModel
        ]
        super().__init__(comment="my test", models=self.models)


class Dataplatform(DeltaCatalog):
    def __init__(self):
        database_list = [
            NewTestDatabase()
        ]
        super().__init__(database_list)


Dataplatform().register_all_databases()
# MyTestDatabase().register_database().register_models()
```

3. New usage pattern in ETL pipelines, compared to previous usage pattern

```python
from models import SampleTableClass

# Usage pattern change
# Previous pattern
df.select(f.col("sample_column"))
# New pattern
df.select(f.col(SampleTableClass.sample_column.name))
```

### Intermediate discussion and conclusions (work-in-progress)

Now clearly, we have added a lot more boilerplate, but this is the current price of explicitly exposing the
dependencies. Hopefully, we may reduce this a bit, so the code is still fairly readable.

