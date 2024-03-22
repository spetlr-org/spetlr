import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.transformers import DropColumnsTransformer
from spetlr.spark import Spark


class TestDropColumnsTransformer(DataframeTestCase):
    def test_drop_columns_transformer(self):
        inputSchema = T.StructType(
            [
                T.StructField("id", T.LongType(), True),
                T.StructField("text1", T.StringType(), True),
                T.StructField("text2", T.StringType(), True),
            ]
        )

        inputData = [
            (
                1,
                "text1",
                "text2",
            ),
        ]

        df_input = Spark.get().createDataFrame(data=inputData, schema=inputSchema)

        expectedData = [
            (1,),
        ]

        df_transformed = DropColumnsTransformer(columnList=["text1", "text2"]).process(
            df_input
        )
        self.assertDataframeMatches(df_transformed, None, expectedData)
