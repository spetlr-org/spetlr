import pyspark.sql.functions as f
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spetlr.etl import Extractor, Loader, Orchestrator, Transformer
from spetlr.etl.types import dataset_group
from spetlr.spark import Spark


class OfficeEmployeeExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize(
                [
                    ("1", "Michael Scott", "Regional Manager"),
                    ("2", "Dwight K. Schrute", "Assistant to the Regional Manager"),
                    ("3", "Jim Halpert", "Salesman"),
                    ("4", "Pam Beesly", "Receptionist"),
                ]
            ),
            StructType(
                [
                    StructField("id", StringType()),
                    StructField("name", StringType()),
                    StructField("position", StringType()),
                ]
            ),
        )


class OfficeBirthdaysExtractor(Extractor):
    def read(self) -> DataFrame:
        return Spark.get().createDataFrame(
            Spark.get().sparkContext.parallelize(
                [
                    (1, "March 15"),
                    (2, "January 20"),
                    (3, "October 1"),
                    (4, "March 25"),
                ]
            ),
            StructType(
                [
                    StructField("id", IntegerType()),
                    StructField("birthday", StringType()),
                ]
            ),
        )


class IntegerTransformer(Transformer):
    def process(self, df: DataFrame) -> DataFrame:
        return df.withColumn("id", f.col("id").cast(IntegerType()))


class JoinTransformer(Transformer):
    def process_many(self, dataset: dataset_group) -> DataFrame:
        df_employee = dataset["df_employee_transformed"]
        df_birthdays = dataset["df_birthdays"]

        return df_employee.join(other=df_birthdays, on="id")


class NoopLoader(Loader):
    def save(self, df: DataFrame) -> None:
        df.write.format("noop").mode("overwrite").save()
        df.printSchema()
        df.show()


print("ETL Orchestrator using two non consuming transformers")
etl = (
    Orchestrator()
    .extract_from(OfficeEmployeeExtractor(dataset_key="df_employee"))
    .extract_from(OfficeBirthdaysExtractor(dataset_key="df_birthdays"))
    .transform_with(
        IntegerTransformer(
            dataset_input_keys="df_employee",
            dataset_output_key="df_employee_transformed",
            consume_inputs=False,
        )
    )
    .transform_with(
        JoinTransformer(
            dataset_input_keys=["df_employee_transformed", "df_birthdays"],
            dataset_output_key="df_final",
        )
    )
    .load_into(NoopLoader(dataset_input_keys="df_final"))
)
etl.execute()
