from pyspark.sql.utils import AnalysisException
from spetlrtools.testing import DataframeTestCase

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.etl import Orchestrator
from spetlr.etl.extractors import SimpleExtractor
from spetlr.etl.extractors.schema_extractor import SchemaExtractor
from spetlr.etl.loaders import SimpleLoader
from spetlr.schema_manager import SchemaManager
from spetlr.spark import Spark

from .extras.schemas import test_schema


class DeltaTests(DataframeTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.set_debug()

    def test_01_configure(self):
        sc = SchemaManager()
        sc.register_schema("TEST_SCHEMA", test_schema)

        tc = Configurator()

        tc.register("MyDb", {"name": "TestDb{ID}"})

        tc.register(
            "MyTbl",
            {"name": "TestDb{ID}.TestTbl"},
        )

        tc.register(
            "MyTbl2",
            {
                "name": "TestDb{ID}.TestTbl2",
            },
        )

        tc.register(
            "MyTbl4",
            {
                "name": "TestDb{ID}.TestTbl4",
            },
        )

        tc.register(
            "MyTbl5",
            {
                "name": "TestDb{ID}.TestTbl5",
            },
        )

        tc.register(
            "MyTbl6",
            {
                "name": "TestDb{ID}.TestTbl6",
            },
        )

        tc.register(
            "MyTbl7",
            {
                "name": "TestDb{ID}.TestTbl7",
            },
        )

        tc.register(
            "MyTblWithSchema",
            {
                "name": "TestDb{ID}.MyTblWithSchema",
                "schema": "TEST_SCHEMA",
            },
        )

        # test instantiation without error
        db = DbHandle.from_tc("MyDb")
        DeltaHandle.from_tc("MyTbl")
        DeltaHandle.from_tc("MyTbl2")
        DeltaHandle.from_tc("MyTbl4")
        DeltaHandle.from_tc("MyTbl5")
        DeltaHandle.from_tc("MyTbl6")
        DeltaHandle.from_tc("MyTbl7")
        DeltaHandle.from_tc("MyTblWithSchema")

        db.create()

    def test_02_write(self):
        dh = DeltaHandle.from_tc("MyTbl")
        dh2 = DeltaHandle.from_tc("MyTbl2")
        dh.drop_and_delete()
        dh2.drop_and_delete()

        df = Spark.get().createDataFrame([(1, "a"), (2, "b")], "id int, name string")

        dh.overwrite(df, mergeSchema=True)
        dh.append(df, mergeSchema=False)  # schema matches

        df = Spark.get().createDataFrame(
            [(1, "a", "yes"), (2, "b", "no")],
            """
            id int,
            name string,
            response string
            """,
        )

        dh.append(df, mergeSchema=True)

    # # This test asserted that the data persisted for an external table
    # # Now that tables are managed, this no longer works
    # def test_03_create(self):
    #     db = DbHandle.from_tc("MyDb")
    #     db.drop_cascade()
    #     db.create()
    #
    #     dh = DeltaHandle.from_tc("MyTbl")
    #     dh.create_hive_table()
    #
    #     # test hive access:
    #     df = dh.read()
    #     self.assertTrue(6, df.count())

    def test_04_read(self):
        df = DeltaHandle.from_tc("MyTbl").read()
        self.assertEqual(6, df.count())

    def test_041_schema(self):
        """Schema Extractor returns zero lines, but preserves the schema,
        giving more readable orchestrators"""
        df1 = SimpleExtractor(DeltaHandle.from_tc("MyTbl"), "MyTbl").read()
        self.assertEqual(6, df1.count())

        df2 = SchemaExtractor(DeltaHandle.from_tc("MyTbl"), "MyTbl").read()
        self.assertEqual(0, df2.count())

        self.assertEqual(df1.schema, df2.schema)

    def test_05_truncate(self):
        dh = DeltaHandle.from_tc("MyTbl")
        dh.truncate()
        df = dh.read()
        self.assertEqual(0, df.count())

    def test_06_etl(self):
        o = Orchestrator()
        o.extract_from(
            SimpleExtractor(DeltaHandle.from_tc("MyTbl"), dataset_key="MyTbl")
        )
        o.load_into(SimpleLoader(DeltaHandle.from_tc("MyTbl"), mode="overwrite"))
        o.execute()

    def test_08_delete(self):
        dh = DeltaHandle.from_tc("MyTbl")
        dh.drop_and_delete()

        with self.assertRaises(AnalysisException):
            dh.read()

    def test_09_partitioning(self):
        dh = DeltaHandle.from_tc("MyTbl4")
        Spark.get().sql(
            f"""
            CREATE TABLE {dh.get_tablename()}
            (
            colA string,
            colB int,
            payload string
            )
            PARTITIONED BY (colB,colA)
        """
        )

        self.assertEqual(dh.get_partitioning(), ["colB", "colA"])

        dh2 = DeltaHandle.from_tc("MyTbl5")
        Spark.get().sql(
            f"""
            CREATE TABLE {dh2.get_tablename()}
            (
            colA string,
            colB int,
            payload string
            )
        """
        )

        self.assertEqual(dh2.get_partitioning(), [])

    def test_10_cluster(self):
        dh = DeltaHandle.from_tc("MyTbl6")
        Spark.get().sql(
            f"""
            CREATE TABLE {dh.get_tablename()}
            (
            colA string,
            colB int,
            payload string
            )
            CLUSTER BY (colB,colA)
        """
        )

        self.assertEqual(dh.get_cluster(), ["colB", "colA"])

        dh2 = DeltaHandle.from_tc("MyTbl7")
        Spark.get().sql(
            f"""
            CREATE TABLE {dh2.get_tablename()}
            (
            colA string,
            colB int,
            payload string
            )
        """
        )

        self.assertEqual(dh2.get_cluster(), [])

    def test_11_get_schema(self):
        # test instantiation without error
        dh = DeltaHandle.from_tc("MyTblWithSchema")

        self.assertEqualSchema(test_schema, dh.get_schema())

    def test_12_set_schema(self):
        # test instantiation without error
        dh = DeltaHandle.from_tc("MyTblWithSchema")

        dh.set_schema(None)

        self.assertIsNone(dh.get_schema())
