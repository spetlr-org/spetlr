import unittest

import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.transformers import ReplaceEmptyStringTransformer
from spetlr.utils import DataframeCreator


class TestReplaceEmptyStringTransformer(DataframeTestCase):
    def test_replace_empty_string_transformer(self):
        test_schema = T.StructType(
            [
                T.StructField("FirstName", T.StringType(), True),
                T.StructField("LastName", T.StringType(), True),
            ]
        )

        input_data = [
            ("Arne", ""),
            ("", "Petersen"),
            ("", ""),
            ("Arne", "Petersen"),
            (" ", ""),
        ]

        df_input = DataframeCreator().make(
            data=input_data,
            schema=test_schema,
        )

        df_transformed = ReplaceEmptyStringTransformer().process(df_input)

        expected_data = [
            (
                "Arne",
                None,
            ),
            (
                None,
                "Petersen",
            ),
            (
                None,
                None,
            ),
            (
                "Arne",
                "Petersen",
            ),
            (
                " ",
                None,
            ),
        ]

        self.assertDataframeMatches(df=df_transformed, expected_data=expected_data)


if __name__ == "__main__":
    unittest.main()
