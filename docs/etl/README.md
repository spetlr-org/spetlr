# ETL Orchestrator

## Introduction

This module contains components for implementing elegant ETL operations using the **[OETL Design Pattern](#OETL)**.

## OETL

Short for **Orchestrated Extract-Transform-Load** is pattern that takes the ideas behind variations of the Model-View-Whatever design pattern

![Orchestrated ETL](etl-orchestrator.png)

The **Orchestrator** is responsible for conducting the interactions between the **Extractor** -> **Transformer** -> **Loader**. 
The **Ochestrator** reads data from the **Extractor** then uses the result as a parameter to calling the **Transformer** and saves the transformed result into the **Loader**. The **Transformer** can be optional as there are scenarios where data transformation is not needed (i.e. raw data ingestion to a landing zone)

Each layer may have a single or multiple implementations, and this is handled by different implementations of the **Ochestrator**

## OrchestratorFactory

This library provides a common simple implementation and base classes for implementing the OETL design pattern. To simplify object construction, we provide the `OrchestratorFactory` class under `atc.etl`

```python
def create(extractor: Extractor, transformer: Transformer, loader: Loader):
def create_for_raw_ingestion(extractor: Extractor, loader: Loader):
def create_for_multiple_sources(extractors: [Extractor], transformer: MultiInputTransformer, loader: Loader):
def create_for_multiple_transformers(extractor: Extractor, transformers: [Transformer], loader: Loader):
```

## Usage examples:

Here are some example usages and implementations of the ETL class provided

### Example-1

```python
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as f

from atc.etl import Extractor, Transformer, Loader, OrchestratorFactory, DelegatingTransformer
from atc.spark import Spark


class GuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize([
                ('1', 'Fender', 'Telecaster', '1950'),
                ('2', 'Gibson', 'Les Paul', '1959'),
                ('3', 'Ibanez', 'RG', '1987')
            ]),
            StructType([
                StructField('id', StringType()),
                StructField('brand', StringType()),
                StructField('model', StringType()),
                StructField('year', StringType()),
            ]))


class BasicTransformer(Transformer):
    def process(self, df: DataFrame) -> DataFrame:
        print('Current DataFrame schema')
        df.printSchema()

        df = df.withColumn('id', f.col('id').cast(IntegerType()))
        df = df.withColumn('year', f.col('year').cast(IntegerType()))

        print('New DataFrame schema')
        df.printSchema()
        return df


class NoopLoader(Loader):
    def save(self, df: DataFrame) -> DataFrame:
        df.write.format('noop').mode('overwrite').save()
        return df


print('ETL Orchestrator using a single simple transformer')
etl = OrchestratorFactory.create(GuitarExtractor(), BasicTransformer(), NoopLoader())
result = etl.execute()
result.printSchema()
result.show()
```

The code above produces the following output:

```
Original DataFrame schema
root
 |-- id: string (nullable = true)
 |-- brand: string (nullable = true)
 |-- model: string (nullable = true)
 |-- year: string (nullable = true)

New DataFrame schema
root
 |-- id: integer (nullable = true)
 |-- brand: string (nullable = true)
 |-- model: string (nullable = true)
 |-- year: integer (nullable = true)

+---+------+----------+----+
| id| brand|     model|year|
+---+------+----------+----+
|  1|Fender|Telecaster|1950|
|  2|Gibson|  Les Paul|1959|
|  3|Ibanez|        RG|1987|
+---+------+----------+----+
```

### Example-2

Using the [code above](#Example-1) as reference, the transformation code can be improved to be more generic and reused for other operations. 
Here's an example of implementing a `Transformer` that is reused to change the data type of a given column,
where the column name is parameterized

```python
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Extractor, Transformer, Loader, OrchestratorFactory, MultiInputTransformer
from atc.spark import Spark


class GuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize([
                ('1', 'Fender', 'Telecaster', '1950'),
                ('2', 'Gibson', 'Les Paul', '1959'),
                ('3', 'Ibanez', 'RG', '1987')
            ]),
            StructType([
                StructField('id', StringType()),
                StructField('brand', StringType()),
                StructField('model', StringType()),
                StructField('year', StringType()),
            ]))


class IntegerColumnTransformer(Transformer):
    def __init__(self, col_name: str):
        self.col_name = col_name

    def process(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(self.col_name, f.col(self.col_name).cast(IntegerType()))
        return df


class NoopLoader(Loader):
    def save(self, df: DataFrame) -> DataFrame:
        df.write.format('noop').mode('overwrite').save()
        return df


print('ETL Orchestrator using multiple transformers')
etl = OrchestratorFactory.create_for_multiple_transformers(GuitarExtractor(),
                                                           [IntegerColumnTransformer('id'), IntegerColumnTransformer('year')],
                                                           NoopLoader())
result = etl.execute()
result.printSchema()
result.show()

```

### Example-3

There are scenarios that you might have to ingest data from multiple data sources and merge them into a single dataframe. Here's an example of have multiple `Extractor` implementation encapsulated in an instance of `DelegatingExtractor` and applying transformations using the `MultiInputTransformer`

The `read()` function in `DelegatingExtractor` will return a dictionary that uses the type name of the `Extractor` as the key, and a `DataFrame` as its value

`MultiInputTransformer` provides the function `process_many(dataset: {})` and returns a single `DataFrame`

```python
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from atc.etl import Extractor, Loader, OrchestratorFactory, MultiInputTransformer
from atc.spark import Spark


class AmericanGuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize([
                ('1', 'Fender', 'Telecaster', '1950'),
                ('2', 'Gibson', 'Les Paul', '1959')
            ]),
            StructType([
                StructField('id', StringType()),
                StructField('brand', StringType()),
                StructField('model', StringType()),
                StructField('year', StringType()),
            ]))


class JapaneseGuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize([
                ('3', 'Ibanez', 'RG', '1987'),
                ('4', 'Takamine', 'Pro Series', '1959')
            ]),
            StructType([
                StructField('id', StringType()),
                StructField('brand', StringType()),
                StructField('model', StringType()),
                StructField('year', StringType()),
            ]))


class CountryOfOriginTransformer(MultiInputTransformer):
    def process_many(self, dataset: {}) -> DataFrame:
        usa_df = dataset['AmericanGuitarExtractor'].withColumn('country', f.lit('USA'))
        jap_df = dataset['JapaneseGuitarExtractor'].withColumn('country', f.lit('Japan'))
        return usa_df.union(jap_df)


class NoopLoader(Loader):
    def save(self, df: DataFrame) -> DataFrame:
        df.write.format('noop').mode('overwrite').save()
        return df


print('ETL Orchestrator using multiple extractors')
etl = OrchestratorFactory.create_for_multiple_sources([AmericanGuitarExtractor(), JapaneseGuitarExtractor()],
                                                      CountryOfOriginTransformer(),
                                                      NoopLoader())
result = etl.execute()
result.printSchema()
result.show()
```

The code above produces the following output:

```
root
 |-- id: string (nullable = true)
 |-- brand: string (nullable = true)
 |-- model: string (nullable = true)
 |-- year: string (nullable = true)
 |-- country: string (nullable = false)

+---+--------+----------+----+-------+
| id|   brand|     model|year|country|
+---+--------+----------+----+-------+
|  1|  Fender|Telecaster|1950|    USA|
|  2|  Gibson|  Les Paul|1959|    USA|
|  3|  Ibanez|        RG|1987|  Japan|
|  4|Takamine|Pro Series|1959|  Japan|
+---+--------+----------+----+-------+
```

### Example-4

Here's an example of data raw ingestion without applying any transformations

```python
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from atc.etl import Extractor, Loader, OrchestratorFactory
from atc.spark import Spark


class GuitarExtractor(Extractor):
  def read(self) -> DataFrame:
    return Spark.get().createDataFrame(
      Spark.get().sparkContext.parallelize([
        ('1', 'Fender', 'Telecaster', '1950'),
        ('2', 'Gibson', 'Les Paul', '1959'),
        ('3', 'Ibanez', 'RG', '1987')
      ]),
      StructType([
        StructField('id', StringType()),
        StructField('brand', StringType()),
        StructField('model', StringType()),
        StructField('year', StringType()),
      ]))


class NoopLoader(Loader):
  def save(self, df: DataFrame) -> DataFrame:
    df.write.format('noop').mode('overwrite').save()
    return df


print('ETL Orchestrator with no transformer')
etl = OrchestratorFactory.create_for_raw_ingestion(GuitarExtractor(), NoopLoader())
result = etl.execute()
result.printSchema()
result.show()
```

### Example-5

Here's an example of writing the transformed data into multiple destinations

```python
import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Extractor, Transformer, Loader, OrchestratorFactory
from atc.spark import Spark


class GuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize([
                ('1', 'Fender', 'Telecaster', '1950'),
                ('2', 'Gibson', 'Les Paul', '1959'),
                ('3', 'Ibanez', 'RG', '1987')
            ]),
            StructType([
                StructField('id', StringType()),
                StructField('brand', StringType()),
                StructField('model', StringType()),
                StructField('year', StringType()),
            ]))


class BasicTransformer(Transformer):
    def process(self, df: DataFrame) -> DataFrame:
        print('Current DataFrame schema')
        df.printSchema()

        df = df.withColumn('id', f.col('id').cast(IntegerType()))
        df = df.withColumn('year', f.col('year').cast(IntegerType()))

        print('New DataFrame schema')
        df.printSchema()
        return df


class NoopSilverLoader(Loader):
    def save(self, df: DataFrame) -> DataFrame:
        df.write.format('noop').mode('overwrite').save()
        return df


class NoopGoldLoader(Loader):
    def save(self, df: DataFrame) -> DataFrame:
        df.write.format('noop').mode('overwrite').save()
        return df


print('ETL Orchestrator using multiple loaders')
etl = OrchestratorFactory.create_for_multiple_destinations(GuitarExtractor(),
                                                           BasicTransformer(),
                                                           [NoopSilverLoader(), NoopGoldLoader()])
result = etl.execute()
result.printSchema()
result.show()
```
