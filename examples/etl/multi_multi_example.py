import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spetlr.etl import Extractor, Loader, Orchestrator, Transformer
from spetlr.spark import Spark


class AmericanGuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize(
                [
                    ("1", "Fender", "Telecaster", "1950"),
                    ("2", "Gibson", "Les Paul", "1959"),
                ]
            ),
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("brand", StringType()),
                    StructField("model", StringType()),
                    StructField("year", StringType()),
                ]
            ),
        )


class JapaneseGuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize(
                [("3", "Ibanez", "RG", "1987"), ("4", "Takamine", "Pro Series", "1959")]
            ),
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("brand", StringType()),
                    StructField("model", StringType()),
                    StructField("year", StringType()),
                ]
            ),
        )


class CountryOfOriginTransformer(Transformer):
    def process_many(self, dataset: {}) -> DataFrame:
        usa_df = dataset["AmericanGuitarExtractor"].withColumn("country", f.lit("USA"))
        jap_df = dataset["JapaneseGuitarExtractor"].withColumn(
            "country", f.lit("Japan")
        )
        return usa_df.union(jap_df)


class BasicTransformer(Transformer):
    def process(self, df: DataFrame) -> DataFrame:
        print("Current DataFrame schema")
        df.printSchema()

        df = df.withColumn("id", f.col("id").cast(IntegerType()))
        df = df.withColumn("year", f.col("year").cast(IntegerType()))

        print("New DataFrame schema")
        df.printSchema()
        return df


class NoopSilverLoader(Loader):
    def save(self, df: DataFrame) -> None:
        df.write.format("noop").mode("overwrite").save()


class NoopGoldLoader(Loader):
    def save(self, df: DataFrame) -> None:
        df.write.format("noop").mode("overwrite").save()
        df.printSchema()
        df.show()


print("ETL Orchestrator using multiple loaders")
etl = (
    Orchestrator()
    .extract_from(AmericanGuitarExtractor())
    .extract_from(JapaneseGuitarExtractor())
    .transform_with(CountryOfOriginTransformer())
    .transform_with(BasicTransformer())
    .load_into(NoopSilverLoader())
    .load_into(NoopGoldLoader())
)
etl.execute()
