import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr import Configurator
from spetlr.delta import DeltaHandle
from spetlr.sql import SqlHandle
from spetlr.utils import delete_mismatched_schemas
from tests.cluster.delta import extras as extrasdelta
from tests.cluster.delta.SparkExecutor import SparkSqlExecutor
from tests.cluster.sql import extras as extrassql
from tests.cluster.sql.DeliverySqlExecutor import DeliverySqlExecutor
from tests.cluster.sql.DeliverySqlServer import DeliverySqlServer


class DeleteSchemaMismatch(DataframeTestCase):
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

    initial_schema_sql = T.StructType(
        [
            T.StructField("testcolumn", T.IntegerType(), True),
        ]
    )

    changed_schema_sql = T.StructType(
        [
            T.StructField("testcolumn", T.IntegerType(), True),
            T.StructField("testcolumn2", T.IntegerType(), True),
        ]
    )

    @classmethod
    def setUpClass(cls) -> None:
        Configurator().add_resource_path(extrasdelta)
        Configurator().add_resource_path(extrassql)
        Configurator().set_debug()

    def test_01_no_change_delta(self):
        dh = DeltaHandle.from_tc("MismatchSparkTestTable")

        SparkSqlExecutor().execute_sql_file("initial_schema")

        self.assertEqual(dh.read().schema, self.initial_schema_delta)

        delete_mismatched_schemas(
            sqlserver_executor=SparkSqlExecutor(), sql_files_pattern="initial_schema"
        )
        SparkSqlExecutor().execute_sql_file("initial_schema")

        self.assertEqual(dh.read().schema, self.initial_schema_delta)

    def test_02_change_delta(self):
        dh = DeltaHandle.from_tc("MismatchSparkTestTable")

        SparkSqlExecutor().execute_sql_file("initial_schema")

        self.assertEqual(dh.read().schema, self.initial_schema_delta)

        delete_mismatched_schemas(
            sqlserver_executor=SparkSqlExecutor(), sql_files_pattern="changed_schema"
        )
        SparkSqlExecutor().execute_sql_file("changed_schema")

        self.assertEqual(dh.read().schema, self.changed_schema_delta)

    def test_03_no_change_sql(self):
        name = Configurator().table_name("MismatchSparkTestTable")
        sqlh = SqlHandle(name, DeliverySqlServer())

        DeliverySqlExecutor().execute_sql_file("initial_schema")

        self.assertEqual(sqlh.read().schema, self.initial_schema_sql)

        delete_mismatched_schemas(
            sqlserver_executor=SparkSqlExecutor(), sql_files_pattern="initial_schema"
        )
        SparkSqlExecutor().execute_sql_file("initial_schema")

        self.assertEqual(sqlh.read().schema, self.initial_schema_sql)

    def test_03_change_sql(self):
        name = Configurator().table_name("MismatchSparkTestTable")
        sqlh = SqlHandle(name, DeliverySqlServer())

        DeliverySqlExecutor().execute_sql_file("initial_schema")

        self.assertEqual(sqlh.read().schema, self.initial_schema_sql)

        delete_mismatched_schemas(
            sqlserver_executor=SparkSqlExecutor(), sql_files_pattern="changed_schema"
        )
        SparkSqlExecutor().execute_sql_file("changed_schema")

        self.assertEqual(sqlh.read().schema, self.changed_schema_sql)
