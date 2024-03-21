import unittest

import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.transformers import FilterNullValuesTransformer
from spetlr.utils import DataframeCreator


class TestFilterNullValuesTransformer(DataframeTestCase):
    def test_with_one_column(self):
        test_schema = T.StructType(
            [
                T.StructField("FirstName", T.StringType(), True),
                T.StructField("LastName", T.StringType(), True),
            ]
        )

        test_data = [
            (None, "Petersen"),
            ("Arne", None),
            ("Arne", "Petersen"),
            (None, None),
            ("", "Petersen"),  # This row is for testing empty string on transformer
        ]

        df_test = DataframeCreator().make(
            data=test_data,
            schema=test_schema,
        )

        df_transformed = FilterNullValuesTransformer(columns=["FirstName"]).process(
            df_test
        )

        expected_data = [
            (
                "Arne",
                None,
            ),
            (
                "Arne",
                "Petersen",
            ),
            (
                "",
                "Petersen",
            ),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )

    def test_with_multiple_columns(self):
        test_schema = T.StructType(
            [
                T.StructField("FirstName", T.StringType(), True),
                T.StructField("LastName", T.StringType(), True),
            ]
        )

        test_data = [
            (None, "Petersen"),
            ("Arne", None),
            ("Arne", "Petersen"),
            (None, None),
            ("", "Petersen"),  # This row is for testing empty string on transformer
        ]

        df_test = DataframeCreator().make(
            data=test_data,
            schema=test_schema,
        )

        df_transformed = FilterNullValuesTransformer(
            columns=["FirstName", "LastName"]
        ).process(df_test)

        expected_data = [
            (
                "Arne",
                "Petersen",
            ),
            (
                "",
                "Petersen",
            ),
        ]

        self.assertDataframeMatches(
            df=df_transformed,
            expected_data=expected_data,
        )


if __name__ == "__main__":
    unittest.main()
