import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

from atc.etl import Extractor, Loader, Orchestrator, Transformer, dataset_group
from atc.spark import Spark


class AmericanGuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize(
                [
                    ("1", "Fender", "Telecaster", "1950"),
                    ("2", "Gibson", "Les Paul", "1959"),
                ]
            ),
            T.StructType(
                [
                    T.StructField("id", T.StringType()),
                    T.StructField("brand", T.StringType()),
                    T.StructField("model", T.StringType()),
                    T.StructField("year", T.StringType()),
                ]
            ),
        )


class JapaneseGuitarExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize(
                [
                    ("3", "Ibanez", "RG", "1987"),
                    ("4", "Takamine", "Pro Series", "1959"),
                ]
            ),
            T.StructType(
                [
                    T.StructField("id", T.StringType()),
                    T.StructField("brand", T.StringType()),
                    T.StructField("model", T.StringType()),
                    T.StructField("year", T.StringType()),
                ]
            ),
        )


class CountryOfOriginTransformer(Transformer):
    def process_many(self, dataset: dataset_group) -> DataFrame:
        usa_df = dataset["AmericanGuitarExtractor"].withColumn("country", F.lit("USA"))
        jap_df = dataset["JapaneseGuitarExtractor"].withColumn(
            "country", F.lit("Japan")
        )
        return usa_df.union(jap_df)


class OrchestratorLoader(Loader):
    def __init__(self, orchestrator: Orchestrator):
        super().__init__()
        self.orchestrator = orchestrator

    def save_many(self, datasets: dataset_group) -> None:
        self.orchestrator.execute(datasets)


class NoopLoader(Loader):
    def save(self, df: DataFrame) -> None:
        df.write.format("noop").mode("overwrite").save()
        df.printSchema()
        df.show()


print("ETL Orchestrator using composit inner orchestrator")
etl_inner = (
    Orchestrator().transform_with(CountryOfOriginTransformer()).load_into(NoopLoader())
)

etl_outer = (
    Orchestrator()
    .extract_from(AmericanGuitarExtractor())
    .extract_from(JapaneseGuitarExtractor())
    .load_into(OrchestratorLoader(etl_inner))
)

etl_outer.execute()
