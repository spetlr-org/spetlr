import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.spark import Spark
from spetlr.transformers import CleanColumnNamesTransformer


class TestCleanColumnNamesTransformer(DataframeTestCase):
    """
    Testing the CleanColumnNamesTransformer.
    """

    input_schema = T.StructType(
        [
            T.StructField("col (col)", T.StringType(), True),
            T.StructField("col ", T.StringType(), True),
            T.StructField("col three", T.StringType(), True),
        ]
    )
    input_data = []

    def test_clean_column_names_transformer(self):
        # Creating an input dataframe using the specified schema

        input_df = Spark.get().createDataFrame(
            data=self.input_data, schema=self.input_schema
        )

        # Transforming the input data
        df_transformed = CleanColumnNamesTransformer().process(df=input_df)

        # Specifying the expected schema
        expected_schema = T.StructType(
            [
                T.StructField("col_col", T.StringType(), True),
                T.StructField("col", T.StringType(), True),
                T.StructField("col_three", T.StringType(), True),
            ]
        )

        # Creating expected synthetic data
        expected_data = []

        # Creating a dataframe with the expected output using the specified schema
        df_expected = Spark.get().createDataFrame(expected_data, expected_schema)

        # Performing the Equality test
        self.assertEqualSchema(df_transformed.schema, df_expected.schema)
        self.assertDataframeMatches(df=df_transformed, expected_data=expected_data)

    def test_clean_column_names_transformer_exclude_columns(self):
        # Creating an input dataframe using the specified schema
        input_df = Spark.get().createDataFrame(
            data=self.input_data, schema=self.input_schema
        )

        # Transforming the input data
        df_transformed = CleanColumnNamesTransformer(
            exclude_columns=["col (col)"]
        ).process(df=input_df)

        # Specifying the expected schema
        expected_schema = T.StructType(
            [
                T.StructField("col (col)", T.StringType(), True),
                T.StructField("col", T.StringType(), True),
                T.StructField("col_three", T.StringType(), True),
            ]
        )

        # Creating expected synthetic data
        expected_data = []

        # Creating a dataframe with the expected output using the specified schema
        df_expected = Spark.get().createDataFrame(expected_data, expected_schema)

        # Performing the Equality test
        self.assertEqualSchema(df_transformed.schema, df_expected.schema)
        self.assertDataframeMatches(df=df_transformed, expected_data=expected_data)
