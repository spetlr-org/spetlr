import unittest

from pyspark.sql.types import IntegerType, StructField, StructType
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.log.log_transformers import NullLogTransformer
from spetlr.utils import DataframeCreator


class TestNullLogTransformer(DataframeTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        schema = StructType(
            [
                StructField("col_1", IntegerType(), True),
                StructField("col_2", IntegerType(), True),
                StructField("col_3", IntegerType(), True),
            ]
        )

        data_1 = [
            (1, None, 1),
            (2, None, 2),
            (3, 3, 3),
            (4, 4, 4),
            (5, 5, 5),
        ]

        data_2 = [
            (1, 1, 1),
            (2, 2, 2),
            (3, 3, 3),
        ]

        df_1 = DataframeCreator.make(schema, data_1)
        df_2 = DataframeCreator.make(schema, data_2)

        cls.dataset = {"df_1": df_1, "df_2": df_2}

    def test_log_01(self) -> None:
        df = NullLogTransformer(
            log_name="test_log_name",
            column_name="col_2",
        ).log(self.dataset["df_1"])

        self.assertDataframeMatches(
            df=df,
            columns=["Count", "NumberOfNulls", "PercentageOfNulls", "ColumnName"],
            expected_data=[(5, 2, "0.4000", "col_2")],
        )

    def test_log_many_02(self) -> None:
        df = NullLogTransformer(
            log_name="test_log_name",
            column_name="col_2",
        ).log_many(self.dataset)

        self.assertDataframeMatches(
            df=df,
            columns=["Count", "NumberOfNulls", "PercentageOfNulls", "ColumnName"],
            expected_data=[
                (5, 2, "0.4000", "col_2"),
                (3, 0, "0.0000", "col_2"),
            ],
        )


if __name__ == "__main__":
    unittest.main()
