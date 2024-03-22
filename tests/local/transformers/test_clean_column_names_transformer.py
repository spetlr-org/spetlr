import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.transformers import CleanColumnNamesTransformer
from spetlr.exceptions import AmbiguousColumnsAfterCleaning
from spetlr.spark import Spark


class TestCleanColumnNamesTransformer(DataframeTestCase):
    """
    Testing the CleanColumnNamesTransformer.
    """

    input_schema = T.StructType(
        [
            T.StructField("col (col)", T.StringType(), True),
            T.StructField("col ", T.StringType(), True),
            T.StructField("col three", T.StringType(), True),
            T.StructField("col-four", T.StringType(), True),
            T.StructField("col5", T.StringType(), True),
            T.StructField("col<6", T.StringType(), True),
            T.StructField("col.seven", T.StringType(), True),
            T.StructField("..eight.?!", T.StringType(), True),
            T.StructField("??", T.StringType(), True),
            T.StructField("colx@# $%^&*()", T.StringType(), True),
            T.StructField(" col space ", T.StringType(), True),
            T.StructField("col_123_456", T.StringType(), True),
            T.StructField("x", T.StringType(), True),
        ]
    )
    input_data = []

    def test_01_clean_column_names_transformer(self):
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
                T.StructField("col_four", T.StringType(), True),
                T.StructField("col5", T.StringType(), True),
                T.StructField("col6", T.StringType(), True),
                T.StructField("colseven", T.StringType(), True),
                T.StructField("eight", T.StringType(), True),
                T.StructField("", T.StringType(), True),
                T.StructField("colx", T.StringType(), True),
                T.StructField("col_space", T.StringType(), True),
                T.StructField("col_123_456", T.StringType(), True),
                T.StructField("x", T.StringType(), True),
            ]
        )

        # Creating expected synthetic data
        expected_data = []

        # Creating a dataframe with the expected output using the specified schema
        df_expected = Spark.get().createDataFrame(expected_data, expected_schema)

        # Performing the Equality test
        self.assertEqualSchema(df_transformed.schema, df_expected.schema)
        self.assertDataframeMatches(df=df_transformed, expected_data=expected_data)

    def test_02_clean_column_names_transformer_exclude_one_column(self):
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
                T.StructField("col_four", T.StringType(), True),
                T.StructField("col5", T.StringType(), True),
                T.StructField("col6", T.StringType(), True),
                T.StructField("colseven", T.StringType(), True),
                T.StructField("eight", T.StringType(), True),
                T.StructField("", T.StringType(), True),
                T.StructField("colx", T.StringType(), True),
                T.StructField("col_space", T.StringType(), True),
                T.StructField("col_123_456", T.StringType(), True),
                T.StructField("x", T.StringType(), True),
            ]
        )

        # Creating expected synthetic data
        expected_data = []

        # Creating a dataframe with the expected output using the specified schema
        df_expected = Spark.get().createDataFrame(expected_data, expected_schema)

        # Performing the Equality test
        self.assertEqualSchema(df_transformed.schema, df_expected.schema)
        self.assertDataframeMatches(df=df_transformed, expected_data=expected_data)

    def test_03_clean_column_names_transformer_exclude_multiple_columns(self):
        # Creating an input dataframe using the specified schema
        input_df = Spark.get().createDataFrame(
            data=self.input_data, schema=self.input_schema
        )

        # Transforming the input data
        df_transformed = CleanColumnNamesTransformer(
            exclude_columns=["col (col)", "col-four", "??"]
        ).process(df=input_df)

        # Specifying the expected schema
        expected_schema = T.StructType(
            [
                T.StructField("col (col)", T.StringType(), True),
                T.StructField("col", T.StringType(), True),
                T.StructField("col_three", T.StringType(), True),
                T.StructField("col-four", T.StringType(), True),
                T.StructField("col5", T.StringType(), True),
                T.StructField("col6", T.StringType(), True),
                T.StructField("colseven", T.StringType(), True),
                T.StructField("eight", T.StringType(), True),
                T.StructField("??", T.StringType(), True),
                T.StructField("colx", T.StringType(), True),
                T.StructField("col_space", T.StringType(), True),
                T.StructField("col_123_456", T.StringType(), True),
                T.StructField("x", T.StringType(), True),
            ]
        )

        # Creating expected synthetic data
        expected_data = []

        # Creating a dataframe with the expected output using the specified schema
        df_expected = Spark.get().createDataFrame(expected_data, expected_schema)

        # Performing the Equality test
        self.assertEqualSchema(df_transformed.schema, df_expected.schema)
        self.assertDataframeMatches(df=df_transformed, expected_data=expected_data)

    def test_04_ambiguous_columns(self):
        input_schema = T.StructType(
            [
                T.StructField("col ", T.StringType(), True),
                T.StructField("col  ", T.StringType(), True),
            ]
        )
        input_data = []

        # Creating an input dataframe using the specified schema
        input_df = Spark.get().createDataFrame(data=input_data, schema=input_schema)

        # Transforming the input data that should raise error
        with self.assertRaises(AmbiguousColumnsAfterCleaning):
            CleanColumnNamesTransformer().process(df=input_df)
