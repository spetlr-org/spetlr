import unittest
from unittest.mock import MagicMock, patch

from pyspark.sql import Row

from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec


class TestDeltaTableSpec(unittest.TestCase):
    """
    The detail schema can be found here:
    https://learn.microsoft.com/en-us/azure/databricks/delta/table-details

    """

    @patch("spetlr.spark.Spark.get")  # Mock Spark session retrieval
    def test_from_name_with_mocked_describe_detail(self, mock_spark_get):
        """Test DeltaTableSpec.from_name with a mocked DESCRIBE DETAIL output."""

        # Mock Spark session
        mock_spark = MagicMock()
        mock_spark_get.return_value = mock_spark

        # Define the row data as per the example
        mock_row = Row(
            format="delta",
            id="04e1db34-399c-42d8-8c56-a7311f60e73d",
            name="test_catalog.test_db.test_table",
            description=None,
            location="abfss://catalog@heyhey.dfs.core.windows.net/__unitystorage/"
            "catalogs/xxx/tables/yyyy",
            createdAt=None,  # Set as None since timestamps are not needed in this test
            lastModified=None,
            partitionColumns=[],
            clusteringColumns=[],
            numFiles=0,
            sizeInBytes=0,
            properties={"delta.enableDeletionVectors": "true"},
            minReaderVersion=3,
            minWriterVersion=7,
            tableFeatures=["deletionVectors"],
            statistics={"numRowsDeletedByDeletionVectors": 0, "numDeletionVectors": 0},
        )

        # Ensure that
        # spark.sql("DESCRIBE DETAIL ...").collect()
        # returns a **list with a single Row**
        mock_spark.sql.return_value.collect.return_value = [mock_row]

        # Run the test
        spec = DeltaTableSpec.from_name("testcatalog.test_db.test_table")

        # Assert that the location is set to None for managed tables
        self.assertIsNone(
            spec.location,
            "The location should be None " "for managed Unity Catalog tables.",
        )
        self.assertEqual(spec.name, "test_catalog.test_db.test_table")
        self.assertIsNone(spec.comment)  # Description is mapped to `comment`
        self.assertEqual(spec.partitioned_by, [])
        self.assertEqual(spec.cluster_by, [])
        self.assertEqual(
            spec.tblproperties,
            {
                "delta.enableDeletionVectors": "true",
                "delta.minReaderVersion": "3",
                "delta.minWriterVersion": "7",
            },
        )
