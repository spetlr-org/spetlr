# Transformations documentation

Transformations in atc-dataplatform:

* [Concatenate dataframes](#concatenate-data-frames)
* [Fuzzy select](#fuzzy-select-transformer)

## Concatenate data frames
The transformation unions dataframes by appending the dataframes on eachother and keep all columns.

```python
from pyspark.sql import DataFrame
from typing import List

def concat_dfs(dfs: List[DataFrame]) -> DataFrame:   
    ...
```
Usage example: 
``` python
concat_dfs([df1,df2,df3])
```

### Example

This section elaborates on how the `concat_dfs` function works with a small example.

Create three test datasets:
``` python
df1 =   Spark.get().createDataFrame(
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

df2 = Spark.get().createDataFrame(
        Spark.get().sparkContext.parallelize([
            ('1', 'Fender', 'Stratocaster', 'Small'),
            ('2', 'Gibson', 'Les Paul Junior', 'Medium'),
            ('3', 'Ibanez', 'JPM', 'Large')
        ]),
        StructType([
            StructField('id', StringType()),
            StructField('brand', StringType()),
            StructField('model', StringType()),
            StructField('size', StringType()),
        ]))
```
Concatenate (union) the two dataframes:
``` python
 result = concat_dfs([df1,df2])
```

Print the dataframe:

``` python
 result.show()
```

The output is then:
``` python
+------+---+---------------+------+----+
| brand| id|          model|  size|year|
+------+---+---------------+------+----+
|Fender|  1|     Telecaster|  null|1950|
|Gibson|  2|       Les Paul|  null|1959|
|Ibanez|  3|             RG|  null|1987|
|Fender|  1|   Stratocaster| Small|null|
|Gibson|  2|Les Paul Junior|Medium|null|
|Ibanez|  3|            JPM| Large|null|
+------+---+---------------+------+----+
```
See that the columns "brand", "id", "model", "size" (from df2) and "year" (from df1) are added to the dataframe consisting of the union of df1 and df2.

## Fuzzy Select Transformer

The `FuzzySelectTransfromer` is an ETL transformer that can process a single dataframe. Its purpose is to help create
short concise select code that is somewhat insensitive to source columns that are misspelled 
or use different capitalization.

To use, construct the `FuzzySelectTransfromer` with the following arguments:
- `columns` The list of column names in the final dataframe in order.
- `match_cutoff` A cutoff quality in the range [0,1] below which matches will not be accepted. 
  See [difflib arguments](https://docs.python.org/3/library/difflib.html#difflib.get_close_matches) for details.

Under the hood, [difflib](https://docs.python.org/3/library/difflib.html) is used to find a suitable unique mapping
from source to target columns. All column names are converted to lower case before matching.

The association of target to source columns is required to be unique. If the algorithm identifies
multiple matching source columns to a target name, an exception will be raised.

### Example

Given a dataframe `df`, this code renames all columns:
```
>>> df.show()
+----+-----+------+
|inex|count|lables|
+----+-----+------+
|   1|    2|   foo|
|   3|    4|   bar|
+----+-----+------+
>>> from atc.transformers.fuzzy_select import FuzzySelectTransformer
>>> ft = FuzzySelectTransformer(["Index", "Count", "Label"])
>>> ft.process(df).show()
+-----+-----+-----+
|Index|Count|Label|
+-----+-----+-----+
|    1|    2|  foo|
|    3|    4|  bar|
+-----+-----+-----+
```
