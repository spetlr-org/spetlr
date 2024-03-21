import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.transformers import SelectColumnsTransformer
from spetlr.spark import Spark


class TestSelectColumnsTransformer(DataframeTestCase):
    def test_select_columns_transformer(self):
        inputSchema = T.StructType(
            [
                T.StructField("Col1", T.StringType(), True),
                T.StructField("Col2", T.IntegerType(), True),
                T.StructField("Col3", T.DoubleType(), True),
                T.StructField("Col4", T.StringType(), True),
                T.StructField("Col5", T.StringType(), True),
            ]
        )
        inputData = [("Col1Data", 42, 13.37, "Col4Data", "Col5Data")]

        input_df = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        expectedData = [(42, 13.37, "Col4Data")]

        transformed_df = SelectColumnsTransformer(
            columnList=["Col2", "Col3", "Col4"]
        ).process(input_df)

        self.assertDataframeMatches(transformed_df, None, expectedData)
