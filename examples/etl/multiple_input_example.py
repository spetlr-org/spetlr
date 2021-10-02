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
