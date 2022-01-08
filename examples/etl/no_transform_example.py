from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

from atc.etl import Extractor, Loader, Orchestrator
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
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("brand", StringType()),
                    StructField("model", StringType()),
                    StructField("year", StringType()),
                ]
            ),
        )


class NoopLoader(Loader):
    def save(self, df: DataFrame) -> None:
        df.write.format("noop").mode("overwrite").save()


print("ETL Orchestrator with no transformations")
etl = Orchestrator().extract_from(GuitarExtractor()).load_into(NoopLoader())
result = etl.execute()
result.printSchema()
result.show()
