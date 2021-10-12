# Transformations documentation

Transformations in atc-dataplatform:

* [Concatenate dataframes](#concatenate-data-frames)

## Concatenate data frames
The transformation unions dataframes by appending the dataframes on eachother and keep all columns.

(like pd.concat)


```python
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
See that the "size" and "year" columns is added to the dataframe consisting of the union of df1 and df2.
