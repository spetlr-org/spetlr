import uuid as _uuid

import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr.configurator import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.sql import SqlExecutor
from spetlr.utils import DeleteMismatchedSchemas
from tests.cluster.utils import extras


class DeleteSchemaMismatch(DataframeTestCase):
    """
    Test class for DeleteMismatchedSchemas.

    This class contains tests for the `DeleteMismatchedSchemas` utility class.
    It verifies the behavior of the utility when dealing with Delta tables
    that have schema mismatches.

    The class defines the initial and changed schema for testing purposes.
    It also sets up the necessary configuration and resources for the tests.

    NB: This test is special, since it expects production tables.
        Do not use .set_prod() in tests.

    """

    initial_schema_delta = T.StructType(
        [
            T.StructField("a", T.IntegerType(), True),
        ]
    )

    changed_schema_delta = T.StructType(
        [
            T.StructField("a", T.IntegerType(), True),
            T.StructField("b", T.IntegerType(), True),
        ]
    )

    @classmethod
    def setUpClass(cls) -> None:
        Configurator().register("mismatchuuid", str(_uuid.uuid4().hex))
        Configurator().add_resource_path(extras)

        # This extra configuration is necessary
        # since SPETLR tests runs parallel on multiple cluster
        # but in same Databricks workspace
        # The production tables will interfere with each other without an unique id

        # This is a special case.
        # Dont use set_prod in testcases
        Configurator().set_prod()

        cls.executor = SqlExecutor(base_module=extras)

    def test_01_no_change_delta(self):
        """Test scenario with no schema change in Delta table."""
        dbhandle = DbHandle.from_tc("SparkTestDbMismatch")
        dbhandle.drop_cascade()

        # Setup production table

        self.executor.execute_sql_file("initial_schema")

        dh = DeltaHandle.from_tc("MismatchSparkTestTable")

        self.assertEqual(dh.read().schema, self.initial_schema_delta)

        DeleteMismatchedSchemas(
            spark_executor=self.executor, sql_files_pattern="initial_schema"
        )

        self.assertEqual(dh.read().schema, self.initial_schema_delta)

    def test_02_change_delta(self):
        """Test scenario with schema change in Delta table."""
        # Ensure that the database is dropped
        dbhandle = DbHandle.from_tc("SparkTestDbMismatch")
        dbhandle.drop_cascade()

        # Setup production table
        self.executor.execute_sql_file("initial_schema")

        dh = DeltaHandle.from_tc("MismatchSparkTestTable")

        self.assertEqual(dh.read().schema, self.initial_schema_delta)

        DeleteMismatchedSchemas(
            spark_executor=self.executor, sql_files_pattern="changed_schema"
        )
        self.executor.execute_sql_file("changed_schema")

        self.assertEqual(dh.read().schema, self.changed_schema_delta)
