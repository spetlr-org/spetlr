import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.transformers import SimpleEventhubHandleTransformer
from spetlr.spark import Spark


class TestSimpleEventhubHandleTransformer(DataframeTestCase):
    def test_01_basic_transformation(self):
        input_schema = T.StructType(
            [
                T.StructField("id", T.StringType(), True),
                T.StructField("amount", T.IntegerType(), True),
            ]
        )

        input_data = [
            ("a1", 100),
            ("b2", 200),
        ]

        df_input = Spark.get().createDataFrame(data=input_data, schema=input_schema)

        df_transformed = SimpleEventhubHandleTransformer().process(df_input)

        expected_data = [('{"id":"a1","amount":100}',), ('{"id":"b2","amount":200}',)]

        expected_schema = T.StructType(
            [
                T.StructField("value", T.StringType(), True),
            ]
        )

        df_expected = Spark.get().createDataFrame(
            data=expected_data, schema=expected_schema
        )

        self.assertEqualSchema(df_transformed.schema, df_expected.schema)
        self.assertDataframeMatches(df_transformed, None, expected_data)
