import unittest
from unittest.mock import patch

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec
from spetlr.deltaspec.exceptions import InvalidSpecificationError


class TestDeltaTableSpecFromSQL(unittest.TestCase):
    @patch("spetlr.configurator.sql.parse_sql.parse_single_sql_statement")
    @patch("spetlr.schema_manager.spark_schema.get_schema")
    def test_from_sql_valid(self, mock_get_schema, mock_parse_sql):
        """Test DeltaTableSpec.from_sql with a valid CREATE TABLE statement."""

        # Mock SQL parsing result
        mock_parse_sql.return_value = {
            "name": "test_db.test_table",
            "format": "delta",
            "schema": {"sql": "id INT, name STRING"},
            "path": "abfss://catalog@storage.dfs.core.windows.net/tables/test_table",
            "comment": "Test Delta Table",
            "options": {"mergeSchema": "true"},
            "partitioned_by": ["id"],
            "cluster_by": [],
            "tblproperties": {
                "delta.minReaderVersion": "2",
                "delta.minWriterVersion": "5",
            },
        }

        # Use a real StructType schema instead of a mock
        expected_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )
        mock_get_schema.return_value = expected_schema

        # Input SQL
        sql = """
        CREATE TABLE test_db.test_table (
            id INT,
            name STRING
        )
        USING DELTA
        LOCATION 'abfss://catalog@storage.dfs.core.windows.net/tables/test_table'
        COMMENT 'Test Delta Table'
        OPTIONS ('mergeSchema'='true')
        PARTITIONED BY (id)
        TBLPROPERTIES ('delta.minReaderVersion'='2', 'delta.minWriterVersion'='5')
        """

        # Run method
        spec = DeltaTableSpec.from_sql(sql)

        # Assertions
        self.assertEqual(spec.name, "test_db.test_table")
        self.assertEqual(
            spec.schema, expected_schema
        )  # Now correctly compares StructType
        self.assertEqual(
            spec.location,
            "abfss://catalog@storage.dfs.core.windows.net/tables/test_table",
        )
        self.assertEqual(spec.comment, "Test Delta Table")
        self.assertEqual(spec.options, {"mergeSchema": "true"})
        self.assertEqual(spec.partitioned_by, ["id"])
        self.assertEqual(spec.cluster_by, [])

        expected_tblproperties = {
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
            "delta.columnMapping.mode": "name",
        }
        self.assertEqual(spec.tblproperties, expected_tblproperties)

    def test_from_sql_invalid_format(self):
        """Test DeltaTableSpec.from_sql with non-delta format (should raise InvalidSpecificationError)."""

        sql = """
        CREATE TABLE test_db.test_table (
            id INT,
            name STRING
        )
        USING PARQUET
        """

        with self.assertRaises(InvalidSpecificationError):
            DeltaTableSpec.from_sql(sql)

    from pyspark.sql.types import IntegerType, StructField, StructType

    def test_from_sql_missing_fields(self):
        """Test DeltaTableSpec.from_sql with missing optional fields."""

        sql = """
        CREATE TABLE test_db.test_table (
            id INT
        )
        USING DELTA
        """

        with patch(
            "spetlr.configurator.sql.parse_sql.parse_single_sql_statement"
        ) as mock_parse_sql, patch(
            "spetlr.schema_manager.spark_schema.get_schema"
        ) as mock_get_schema:
            mock_parse_sql.return_value = {
                "name": "test_db.test_table",
                "format": "delta",
                "schema": {"sql": "id INT"},
                "location": None,  # Explicitly setting location to None
                "comment": None,  # Explicitly setting comment to None
                "options": {},
                "partitioned_by": [],
                "cluster_by": [],
                "tblproperties": {},  # No tblproperties provided, so defaults should be applied
            }

            # Use a real StructType instead of a string
            expected_schema = StructType([StructField("id", IntegerType(), True)])
            mock_get_schema.return_value = expected_schema

            spec = DeltaTableSpec.from_sql(sql)

            # Assertions for missing values
            self.assertEqual(spec.name, "test_db.test_table")
            self.assertEqual(
                spec.schema, expected_schema
            )  # Now correctly compares StructType
            self.assertIsNone(spec.location)  # Should be None when not provided
            self.assertIsNone(spec.comment)  # Should be None when not provided
            self.assertEqual(spec.options, {})
            self.assertEqual(spec.partitioned_by, [])
            self.assertEqual(spec.cluster_by, [])

            # Assert that default tblproperties are set correctly
            expected_tblproperties = {
                "delta.minReaderVersion": "2",
                "delta.minWriterVersion": "5",
                "delta.columnMapping.mode": "name",
            }
            self.assertEqual(spec.tblproperties, expected_tblproperties)


if __name__ == "__main__":
    unittest.main()
