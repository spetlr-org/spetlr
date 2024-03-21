import hashlib

import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.transformers import GenerateMd5ColumnTransformer
from spetlr.spark import Spark


class TestGenerateMd5ColumnTransformer(DataframeTestCase):
    def test_generate_md5_column(self):
        input_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("text", T.StringType(), True),
            ]
        )

        input_data = [
            (1, "text1"),
            (2, "text2"),
        ]

        df_input = Spark.get().createDataFrame(data=input_data, schema=input_schema)

        df_transformed = GenerateMd5ColumnTransformer(
            col_name="md5_col",
            col_list=["id", "text"],
        ).process(df_input)

        expected_data = [
            (1, "text1", hashlib.md5("1text1".encode()).hexdigest()),
            (2, "text2", hashlib.md5("2text2".encode()).hexdigest()),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    def test_generate_md5_column_with_nulls(self):
        input_schema = T.StructType(
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("text", T.StringType(), True),
            ]
        )

        input_data = [
            (1, "text1"),
            (2, None),
        ]

        df_input = Spark.get().createDataFrame(data=input_data, schema=input_schema)

        df_transformed = GenerateMd5ColumnTransformer(
            col_name="md5_col",
            col_list=["id", "text"],
        ).process(df_input)

        expected_data = [
            (1, "text1", hashlib.md5("1text1".encode()).hexdigest()),
            (2, None, hashlib.md5(("2" + "").encode()).hexdigest()),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )
