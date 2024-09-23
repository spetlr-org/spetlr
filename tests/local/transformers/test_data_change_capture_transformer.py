from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from spetlrtools.testing import DataframeTestCase

from spetlr.etl.transformers.data_change_capture_transformer import (
    DataChangeCaptureTransformer,
)
from spetlr.spark import Spark


class CaptureHistoryTransformerTest(DataframeTestCase):
    def setUp(self):
        self.transformer = DataChangeCaptureTransformer("id")
        self.schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("firstName", StringType(), True),
                StructField("lastName", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        self.columns = ["id", "firstName", "lastName", "age"]

        row_1_1 = (1, "Peter", "Plys", 7)
        row_1_2 = (1, "Peter", "Plys", 8)
        row_2_1 = (2, "Peter", "Pan", 14)
        row_2_2 = (2, "Peter", "PanCake", 14)
        row_3_1 = (3, "Ole", "Lukoje", 98)

        # Test data test_01
        self.source1 = [row_1_1]
        self.target1 = []
        self.expected1 = self.source1

        # Test data for test_02
        self.source2 = [row_1_2, row_2_1]
        self.target2 = [row_1_1]
        self.expected2 = self.source2

        # Test data for test_03
        self.source3 = [row_1_2, row_2_2, row_3_1]
        self.target3 = [row_1_2, row_2_1]
        self.expected3 = [row_2_2, row_3_1]

        # Test data for test_04
        self.source4 = [row_1_2, row_2_2, row_3_1]
        self.target4 = [row_1_2, row_2_2, row_3_1]
        self.expected4 = []

        # Test data for test_05
        self.source5 = [row_1_1]
        self.target5 = [row_1_2]
        self.expected5 = []

    def test_01_capture_new_rows(self):
        """Target is empty, return all records from source"""

        df_source = Spark.get().createDataFrame(data=self.source1, schema=self.schema)
        df_target = Spark.get().createDataFrame(data=self.target1, schema=self.schema)

        dataset_group = {"source": df_source, "target": df_target}

        result_df = self.transformer.process_many(dataset_group)

        self.assertDataframeMatches(
            df=result_df, columns=self.columns, expected_data=self.expected1
        )

    def test_02_capture_new_and_modified_rows(self):
        """returns modified and new rows"""

        df_source = Spark.get().createDataFrame(data=self.source2, schema=self.schema)
        df_target = Spark.get().createDataFrame(data=self.target2, schema=self.schema)

        dataset_group = {"source": df_source, "target": df_target}

        result_df = self.transformer.process_many(dataset_group)

        self.assertDataframeMatches(
            df=result_df, columns=self.columns, expected_data=self.expected2
        )

    def test_03_capture_new_and_modified_rows_ignore_existing(self):
        """returns modified and new rows, and ignore existing, that is unchanged."""

        df_source = Spark.get().createDataFrame(data=self.source3, schema=self.schema)
        df_target = Spark.get().createDataFrame(data=self.target3, schema=self.schema)

        dataset_group = {"source": df_source, "target": df_target}

        result_df = self.transformer.process_many(dataset_group)

        self.assertDataframeMatches(
            df=result_df, columns=self.columns, expected_data=self.expected3
        )

    def test_04_no_changes(self):
        """returning dataframe is empty, since no changes have been detected."""

        df_source = Spark.get().createDataFrame(data=self.source4, schema=self.schema)
        df_target = Spark.get().createDataFrame(data=self.target4, schema=self.schema)

        dataset_group = {"source": df_source, "target": df_target}

        result_df = self.transformer.process_many(dataset_group)

        self.assertDataframeMatches(
            df=result_df, columns=self.columns, expected_data=self.expected4
        )

    def test_05_exclude_colc(self):
        """Ignore age-column, and therefore no data expected"""

        df_source = Spark.get().createDataFrame(data=self.source5, schema=self.schema)
        df_target = Spark.get().createDataFrame(data=self.target5, schema=self.schema)

        dataset_group = {"source": df_source, "target": df_target}

        result_df = DataChangeCaptureTransformer(
            primary_key="id", cols_to_exclude=["age"]
        ).process_many(dataset_group)

        self.assertDataframeMatches(
            df=result_df, columns=self.columns, expected_data=self.expected5
        )
