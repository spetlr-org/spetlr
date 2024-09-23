import re
import unittest
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, StructField, StructType
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.log import LogTransformer
from spetlr.etl.types import dataset_group
from spetlr.utils import DataframeCreator


class LogTransformerSubclass(LogTransformer):
    def log(self, df: DataFrame) -> DataFrame:
        return df.select(F.count("*").alias("Count"))

    def log_many(self, dataset: dataset_group) -> DataFrame:
        return super().log_many(dataset)


class TestLogTransformer(DataframeTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.df_empty = DataframeCreator.make(schema=StructType(), data=[()])

        schema = StructType(
            [
                StructField("col_1", IntegerType(), True),
                StructField("col_2", IntegerType(), True),
            ]
        )

        data = [
            (1, 1),
            (2, None),
            (3, 3),
            (4, None),
        ]

        df_with_data_1 = DataframeCreator.make(schema, data)
        df_with_data_2 = DataframeCreator.make(schema, data)

        cls.dataset = {
            "df_1": df_with_data_1,
            "df_2": df_with_data_2,
        }

    def test_log_is_abstractmethod_01(self) -> None:
        self.assertTrue(getattr(LogTransformer.log, "__isabstractmethod__"))

    def test_log_many_is_abstractmethod_02(self) -> None:
        self.assertTrue(getattr(LogTransformer.log_many, "__isabstractmethod__"))

    def test_log_many_default_behaviour_03(self) -> None:
        df = LogTransformerSubclass(log_name="test").log_many(self.dataset)

        self.assertDataframeMatches(
            df=df,
            columns=["Count", "DatasetInputKey"],
            expected_data=[
                (4, "df_1"),
                (4, "df_2"),
            ],
        )

    def test_create_log_04(self) -> None:
        ltsb = LogTransformerSubclass(
            log_name="test_log_name",
        )

        df = ltsb._create_log(self.dataset["df_1"])

        self.assertDataframeMatches(
            df=df,
            columns=[
                # "LogId", tested separately because uuid
                "LogName",
                # "LogTimestamp", tested separately because datetime
                "LogMethodName",
                "col_1",
                "col_2",
            ],
            expected_data=[
                ("test_log_name", "LogTransformerSubclass", 1, 1),
                ("test_log_name", "LogTransformerSubclass", 2, None),
                ("test_log_name", "LogTransformerSubclass", 3, 3),
                ("test_log_name", "LogTransformerSubclass", 4, None),
            ],
        )

        row_1 = df.collect()[0]

        log_id = row_1[0]
        pattern = (
            r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        )
        self.assertTrue(re.match(pattern, log_id))

        log_timestamp = row_1[2]
        self.assertTrue(isinstance(log_timestamp, datetime))

        datetime_now = datetime.utcnow()
        datetime_now_minus_10_minutes = datetime_now - timedelta(minutes=10)
        datetime_now_plus_10_minutes = datetime_now + timedelta(minutes=10)
        self.assertTrue(
            datetime_now_minus_10_minutes < log_timestamp < datetime_now_plus_10_minutes
        )

    def test_process_05(self) -> None:
        df = LogTransformerSubclass(
            log_name="test_log_name",
            dataset_input_keys=["df_key"],
        ).process(self.dataset["df_1"])

        dataset_input_key_generated = df.select("DatasetInputKey").collect()[0][0]
        self.assertEqual(dataset_input_key_generated, "df_key")

    def test_process_many_06(self) -> None:
        pass
        # has not logic to test as 'log_many' and '_create_log' has already been tested


if __name__ == "__main__":
    unittest.main()
