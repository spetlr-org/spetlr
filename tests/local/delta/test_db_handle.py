import unittest
from unittest.mock import MagicMock, patch

from spetlr import Configurator
from spetlr.delta import DbHandle
from spetlr.delta.db_handle import DbHandleInvalidFormat, DbHandleInvalidName


class TestDbHandle(unittest.TestCase):
    """
    Unit tests for the DbHandle class.

    This test suite verifies the following functionalities of the DbHandle class:
    - Creation of a DbHandle instance from a Configurator object.
    - Validation of database names and formats.
    - Execution of database operations (create, drop, drop cascade).
    - Correct interaction with Spark session for executing SQL commands.

    The tests cover both valid and invalid cases to ensure proper error handling.
    """

    def test_from_tc(self):
        """
        Test DbHandle creation from a Configurator.

        Verifies that a DbHandle object is correctly initialized when using the
        `from_tc` class method. This checks if the database name, location, and format
        are properly retrieved from the Configurator.
        """

        c = Configurator()
        c.clear_all_configurations()
        c.register("test_id", dict(name="test_db", path="/path/to/db", format="db"))

        db_handle = DbHandle.from_tc("test_id")

        self.assertEqual(db_handle._name, "test_db")
        self.assertEqual(db_handle._location, "/path/to/db")
        self.assertEqual(db_handle._data_format, "db")

    def test_validate_valid_name(self):
        """
        Test validation of valid database names.

        Ensures that valid database names, including those with a single dot,
        do not raise exceptions and are stored correctly.
        """
        db_handle = DbHandle(name="valid_db")
        self.assertEqual(db_handle._name, "valid_db")

        db_handle_with_dot = DbHandle(name="catalog.valid_db")
        self.assertEqual(db_handle_with_dot._name, "catalog.valid_db")

    def test_validate_invalid_name(self):
        """
        Test validation of invalid database names.

        Ensures that an exception (DbHandleInvalidName) is raised if the
        database name contains multiple dots, making it an invalid format.
        """
        with self.assertRaises(DbHandleInvalidName):
            DbHandle(name="invalid.db.name")  # Too many dots

    def test_validate_invalid_format(self):
        """
        Test validation of unsupported database formats.

        Ensures that an exception (DbHandleInvalidFormat) is raised if a database
        format other than 'db' is provided.
        """
        with self.assertRaises(DbHandleInvalidFormat):
            DbHandle(name="valid_db", data_format="parquet")  # Only "db" is allowed

    @patch("spetlr.spark.Spark.get")
    def test_drop(self, mock_get):
        """
        Test dropping a database.

        Verifies that the correct SQL statement ("DROP DATABASE IF EXISTS ...") is
        executed when the drop() method is called.
        """
        mock_session = MagicMock()
        mock_get.return_value = mock_session  # Mock get() to return a Spark session

        db_handle = DbHandle(name="test_db")
        db_handle.drop()

        mock_session.sql.assert_called_with("DROP DATABASE IF EXISTS test_db;")

    @patch("spetlr.spark.Spark.get")
    def test_drop_cascade(self, mock_get):
        """
        Test dropping a database with CASCADE.

        Verifies that the correct SQL statement ("DROP DATABASE IF EXISTS ... CASCADE")
        is executed when the drop_cascade() method is called.
        """
        mock_session = MagicMock()
        mock_get.return_value = mock_session  # Mock get() to return a Spark session

        db_handle = DbHandle(name="test_db")
        db_handle.drop_cascade()

        mock_session.sql.assert_called_with("DROP DATABASE IF EXISTS test_db CASCADE;")

    @patch("spetlr.spark.Spark.get")
    def test_create_without_location(self, mock_get):
        """
        Test creating a database without specifying a location.

        Ensures that the correct SQL statement ("CREATE DATABASE IF NOT EXISTS ...")
        is executed when the create() method is called without a location.
        """
        mock_session = MagicMock()
        mock_get.return_value = mock_session  # Mock get() to return a Spark session

        db_handle = DbHandle(name="test_db")
        db_handle.create()

        mock_session.sql.assert_called_with("CREATE DATABASE IF NOT EXISTS test_db;")

    @patch("spetlr.spark.Spark.get")
    def test_create_with_location(self, mock_get):
        """ "
        Test creating a database with a specified location.

        Ensures that the correct SQL statement ("CREATE DATABASE IF NOT EXISTS ... LOCATION ...")
        is executed when the create() method is called with a location.
        """
        mock_session = MagicMock()
        mock_get.return_value = mock_session  # Mock get() to return a Spark session

        db_handle = DbHandle(name="test_db", location="/data/location")
        db_handle.create()

        mock_session.sql.assert_called_with(
            "CREATE DATABASE IF NOT EXISTS test_db LOCATION '/data/location';"
        )
