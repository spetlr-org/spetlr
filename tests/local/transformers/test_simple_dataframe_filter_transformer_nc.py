import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.spark import Spark
from spetlr.transformers import DataFrameFilterTransformerNC


class TestDataFrameFilterTransformerNC(DataframeTestCase):
    def test_filter_with_column_name_and_value(self):
        """
        This test accepts col_name and col_value in DataFrameFilterTransformer and
        checks if the output dataframe is correctly filtered.
        """
        input_schema = T.StructType(
            [
                T.StructField("Col1", T.StringType(), True),
                T.StructField("Col2", T.IntegerType(), True),
                T.StructField("Col3", T.DoubleType(), True),
                T.StructField("Col4", T.StringType(), True),
                T.StructField("Col5", T.StringType(), True),
            ]
        )

        input_data1 = ("Col1Data", 42, 13.37, "Col4Data", "Col5Data")
        input_data2 = ("Col1Data_2nd", 43, 23.37, "Col4Data_2nd", "Col5Data_2nd")
        input_data3 = ("Col1Data", 45, 20.15, "Col4Data_3rd", "Col5Data_3rd")

        input_data = [input_data1, input_data2, input_data3]

        input_df = Spark.get().createDataFrame(data=input_data, schema=input_schema)

        transformed_df = DataFrameFilterTransformerNC(
            col_value="Col1Data", col_name="Col1"
        ).process(input_df)

        transformed_df.show()

        expected_data = [
            input_data1,
            input_data3,
        ]

        self.assertDataframeMatches(
            df=transformed_df,
            expected_data=expected_data,
        )
