import pyspark.sql.types as T
from atc_tools.testing import DataframeTestCase

from atc.spark import Spark
from atc.transformers import DropColumnsTransformerNC


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

        df_transformed = DropColumnsTransformerNC(
            columnList=["text1", "text2"]
        ).process(df_input)
        self.assertDataframeMatches(df_transformed, None, expectedData)
