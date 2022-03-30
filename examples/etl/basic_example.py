import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType

from atc.etl import Extractor, Loader, Orchestrator, Transformer
from atc.spark import Spark


class GuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize(
                [
                    ("1", "Fender", "Telecaster", "1950"),
                    ("2", "Gibson", "Les Paul", "1959"),
                    ("3", "Ibanez", "RG", "1987"),
                ]
            ),
            """
            id STRING,
            brand STRING,
            model STRING,
            year STRING
            """,
        )


class BasicTransformer(Transformer):
    def process(self, df: DataFrame) -> DataFrame:
        print("Current DataFrame schema")
        df.printSchema()

        df = df.withColumn("id", f.col("id").cast(IntegerType()))
        df = df.withColumn("year", f.col("year").cast(IntegerType()))

        print("New DataFrame schema")
        df.printSchema()
        return df


class NoopLoader(Loader):
    def save(self, df: DataFrame) -> None:
        df.write.format("noop").mode("overwrite").save()
        df.printSchema()
        df.show()


print("ETL Orchestrator using a single simple transformer")
etl = (
    Orchestrator()
    .extract_from(GuitarExtractor())
    .transform_with(BasicTransformer())
    .load_into(NoopLoader())
)
etl.execute()
