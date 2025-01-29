import unittest
from unittest.mock import MagicMock, patch

from pyspark.sql import DataFrame, Row
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spetlr.delta import DeltaHandle
from spetlr.deltaspec import DeltaTableSpecBase
from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec
from spetlr.deltaspec.DeltaTableSpecDifference import DeltaTableSpecDifference
from spetlr.deltaspec.exceptions import TableSpecNotReadable


class TestDeltaTableSpecMethods(unittest.TestCase):

    def setUp(self):
        """Setup a sample DeltaTableSpec object."""
        self.schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        self.spec = DeltaTableSpec(
            name="test_db.test_table",
            schema=self.schema,
            location="abfss://storage/tables/test_table",
            tblproperties={
                "delta.minReaderVersion": "2",
                "delta.minWriterVersion": "5",
            },
        )

    def test_copy(self):
        """Test that copy() returns an independent object that compares equal to the original."""
        spec_copy = self.spec.copy()

        self.assertEqual(self.spec, spec_copy)
        self.assertIsNot(
            self.spec, spec_copy
        )  # Ensure they are different objects in memory

    def test_get_dh(self):
        """Test that get_dh() returns a valid DeltaHandle with expected values."""
        dh = self.spec.get_dh()

        self.assertIsInstance(dh, DeltaHandle)
        self.assertEqual(
            dh.get_tablename(), self.spec.name
        )  # Ensuring the name is correct
        self.assertEqual(
            dh._location, self.spec.location
        )  # Ensuring the location matches
        self.assertEqual(dh._data_format, "delta")  # Ensuring format is always delta

    def test_compare_to(self):
        """Test that compare_to() returns a DeltaTableSpecDifference object."""

        other_spec = self.spec.copy()

        # Run the method to get the actual result
        result = self.spec.compare_to(other_spec)

        # Ensure the result is a DeltaTableSpecDifference instance
        self.assertIsInstance(result, DeltaTableSpecDifference)

        # Ensure base and target are correctly assigned
        self.assertEqual(result.base.name, other_spec.name)
        self.assertEqual(result.base.schema, other_spec.schema)
        self.assertEqual(result.base.tblproperties, other_spec.tblproperties)
        self.assertEqual(result.base.location, other_spec.location)

        self.assertEqual(result.target.name, self.spec.name)
        self.assertEqual(result.target.schema, self.spec.schema)
        self.assertEqual(result.target.tblproperties, self.spec.tblproperties)
        self.assertEqual(result.target.location, self.spec.location)

    @patch.object(DeltaTableSpec, "compare_to_name")
    def test_compare_to_location(self, mock_compare_to_name):
        """Test that compare_to_location() calls compare_to_name correctly."""

        # Mock return value
        mock_compare_to_name.return_value = "mocked_result"

        # Call the method
        result = self.spec.compare_to_location()

        # Ensure the method returns the expected result
        self.assertEqual(result, "mocked_result")

        # Ensure compare_to_name() was called exactly once
        mock_compare_to_name.assert_called_once()

    @patch.object(DeltaTableSpec, "from_name")
    def test_compare_to_name(self, mock_from_name):
        """Test that compare_to_name() correctly compares with the catalog table."""

        # Mock from_name() to return a copied version of self.spec
        other_spec = self.spec.copy()
        mock_from_name.return_value = other_spec

        # Convert expected mock return value to DeltaTableSpecBase
        expected_base = DeltaTableSpecBase(
            name=other_spec.name,
            schema=other_spec.schema,
            options=other_spec.options,
            partitioned_by=other_spec.partitioned_by,
            cluster_by=other_spec.cluster_by,
            tblproperties=other_spec.tblproperties,
            location=other_spec.location,
            comment=other_spec.comment,
            blankedPropertyKeys=["delta.columnMapping.maxColumnId"],
        )

        expected_target = DeltaTableSpecBase(
            name=self.spec.name,
            schema=self.spec.schema,
            options=self.spec.options,
            partitioned_by=self.spec.partitioned_by,
            cluster_by=self.spec.cluster_by,
            tblproperties=self.spec.tblproperties,
            location=self.spec.location,
            comment=self.spec.comment,
            blankedPropertyKeys=["delta.columnMapping.maxColumnId"],
        )

        # Call the method
        result = self.spec.compare_to_name()

        # Ensure the result is a DeltaTableSpecDifference object
        self.assertIsInstance(result, DeltaTableSpecDifference)

        # Ensure base and target match expectations
        self.assertEqual(
            result.base, expected_base
        )  # Convert mock return to DeltaTableSpecBase
        self.assertEqual(result.target, expected_target)

        # Ensure from_name() was called with the correct table name
        mock_from_name.assert_called_once_with(self.spec.name)

    @patch("spetlr.spark.Spark.get")
    def test_ensure_df_schema(self, mock_spark_get):
        """Test that ensure_df_schema() raises TableSpecNotReadable if schema mismatch occurs."""
        # Create a mock DataFrame with an incompatible schema
        mismatched_schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("email", StringType(), True),  # Different field name
            ]
        )

        mock_df = MagicMock(spec=DataFrame)
        mock_df.schema = mismatched_schema

        with self.assertRaises(TableSpecNotReadable):
            self.spec.ensure_df_schema(mock_df)

    @patch.object(DeltaTableSpec, "compare_to_name")
    def test_is_readable(self, mock_compare_to_name):
        """Test that is_readable() returns the correct boolean result."""

        # Case 1: When is_readable() should return True
        mock_compare_to_name.return_value.is_readable.return_value = True
        result = self.spec.is_readable()
        self.assertTrue(result)

        # Case 2: When is_readable() should return False
        mock_compare_to_name.return_value.is_readable.return_value = False
        result = self.spec.is_readable()
        self.assertFalse(result)

        # Ensure compare_to_name() was called twice (once per case)
        self.assertEqual(mock_compare_to_name.call_count, 2)
