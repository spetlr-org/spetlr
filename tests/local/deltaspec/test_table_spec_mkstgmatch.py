import unittest
from unittest.mock import call, patch

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from spetlr.deltaspec.DeltaTableSpec import DeltaTableSpec
from spetlr.deltaspec.DeltaTableSpecDifference import DeltaTableSpecDifference
from spetlr.spark import Spark


class TestDeltaTableSpecMakeStorageMatch(unittest.TestCase):
    """
    Test suite for the `make_storage_match` method in DeltaTableSpec.

    This test suite covers the following scenarios:

    1. **test_make_storage_match_no_changes**
       - Ensures that `make_storage_match()` does nothing if the table already matches the specification.

    2. **test_make_storage_match_add_column**
       - Verifies that `make_storage_match()` correctly executes an `ALTER TABLE ... ADD COLUMN` statement when needed.

    3. **test_make_storage_match_drop_column**
       - Checks that `make_storage_match()` executes an `ALTER TABLE ... DROP COLUMN` statement when columns should be removed.

    4. **test_make_storage_match_change_column_type**
       - Ensures that `make_storage_match()` executes an `ALTER TABLE ... ALTER COLUMN` statement when column types need modification.

    5. **test_make_storage_match_create_table**
       - Simulates a missing table and verifies that `make_storage_match(allow_table_create=True)` executes a `CREATE TABLE` statement.

    6. **test_make_storage_match_multiple_changes**
       - Ensures `make_storage_match()` correctly applies multiple changes (adding, dropping, altering columns) in a single run.

    Each test mocks Spark SQL execution to verify that the correct SQL statements are generated and executed.
    """

    def setUp(self):
        """Setup a sample DeltaTableSpec object with a basic schema."""
        self.schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

        self.spec = DeltaTableSpec(
            name="test_db.test_table",
            schema=self.schema,  # Now includes a schema
            location="abfss://storage/tables/test_table",
            tblproperties={
                "delta.minReaderVersion": "2",
                "delta.minWriterVersion": "5",
            },
        )

    @patch.object(Spark, "get")
    @patch.object(DeltaTableSpec, "compare_to_name")
    def test_make_storage_match_no_changes(self, mock_compare_to_name, mock_spark_get):
        """Test that make_storage_match() does nothing if no changes are needed."""

        mock_compare_to_name.return_value.is_different.return_value = False

        self.spec.make_storage_match()

        # Ensure no SQL commands were executed
        mock_spark_get.return_value.sql.assert_not_called()

    @patch.object(Spark, "get")
    @patch.object(DeltaTableSpec, "compare_to_name")
    def test_make_storage_match_add_column(self, mock_compare_to_name, mock_spark_get):
        """Test that make_storage_match() adds a column when needed."""

        mock_compare_to_name.return_value.is_different.return_value = True
        mock_compare_to_name.return_value.alter_statements.return_value = [
            "ALTER TABLE test_db.test_table ADD COLUMN new_col STRING"
        ]

        self.spec.make_storage_match(allow_columns_add=True)

        # Ensure the correct SQL was executed
        mock_spark_get.return_value.sql.assert_called_once_with(
            "ALTER TABLE test_db.test_table ADD COLUMN new_col STRING"
        )

    @patch.object(Spark, "get")
    @patch.object(DeltaTableSpec, "compare_to_name")
    def test_make_storage_match_drop_column(self, mock_compare_to_name, mock_spark_get):
        """Test that make_storage_match() drops a column when allowed."""

        mock_compare_to_name.return_value.is_different.return_value = True
        mock_compare_to_name.return_value.alter_statements.return_value = [
            "ALTER TABLE test_db.test_table DROP COLUMN old_col"
        ]

        self.spec.make_storage_match(allow_columns_drop=True)

        # Ensure the correct SQL was executed
        mock_spark_get.return_value.sql.assert_called_once_with(
            "ALTER TABLE test_db.test_table DROP COLUMN old_col"
        )

    @patch.object(Spark, "get")
    @patch.object(DeltaTableSpec, "compare_to_name")
    def test_make_storage_match_change_column_type(
        self, mock_compare_to_name, mock_spark_get
    ):
        """Test that make_storage_match() alters column types when needed."""

        mock_compare_to_name.return_value.is_different.return_value = True
        mock_compare_to_name.return_value.alter_statements.return_value = [
            "ALTER TABLE test_db.test_table ALTER COLUMN some_col TYPE BIGINT"
        ]

        self.spec.make_storage_match(allow_columns_type_change=True)

        # Ensure the correct SQL was executed
        mock_spark_get.return_value.sql.assert_called_once_with(
            "ALTER TABLE test_db.test_table ALTER COLUMN some_col TYPE BIGINT"
        )

    @patch.object(Spark, "get")
    @patch.object(DeltaTableSpec, "compare_to_name")
    def test_make_storage_match_create_table(
        self, mock_compare_to_name, mock_spark_get
    ):
        """Test that make_storage_match() creates the table if it does not exist."""

        # Simulate a missing table by making compare_to_name() return None for base
        mock_compare_to_name.return_value = DeltaTableSpecDifference(
            base=None, target=self.spec
        )

        # Expected CREATE TABLE SQL
        create_sql = self.spec.get_sql_create()

        # Call make_storage_match with table creation enabled
        self.spec.make_storage_match(allow_table_create=True)

        # Ensure the CREATE TABLE SQL was executed
        mock_spark_get.return_value.sql.assert_called_once_with(create_sql)

    @patch.object(Spark, "get")
    @patch.object(DeltaTableSpec, "compare_to_name")
    def test_make_storage_match_multiple_changes(
        self, mock_compare_to_name, mock_spark_get
    ):
        """Test that make_storage_match() executes multiple SQL alterations when needed."""

        mock_compare_to_name.return_value.is_different.return_value = True
        mock_compare_to_name.return_value.alter_statements.return_value = [
            "ALTER TABLE test_db.test_table ADD COLUMN new_col STRING",
            "ALTER TABLE test_db.test_table DROP COLUMN old_col",
            "ALTER TABLE test_db.test_table ALTER COLUMN some_col TYPE BIGINT",
        ]

        self.spec.make_storage_match(
            allow_columns_add=True,
            allow_columns_drop=True,
            allow_columns_type_change=True,
        )

        # Convert expected calls into `call()` objects to match actual calls
        expected_calls = [
            call("ALTER TABLE test_db.test_table ADD COLUMN new_col STRING"),
            call("ALTER TABLE test_db.test_table DROP COLUMN old_col"),
            call("ALTER TABLE test_db.test_table ALTER COLUMN some_col TYPE BIGINT"),
        ]

        # Ensure all expected SQL commands were executed
        mock_spark_get.return_value.sql.assert_has_calls(expected_calls, any_order=True)
