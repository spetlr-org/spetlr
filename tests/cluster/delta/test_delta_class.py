import unittest

from pyspark.sql.utils import AnalysisException

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.etl import Orchestrator
from spetlr.etl.extractors import SimpleExtractor
from spetlr.etl.extractors.schema_extractor import SchemaExtractor
from spetlr.etl.loaders import SimpleLoader
from spetlr.spark import Spark


class DeltaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.set_debug()

    def test_01_configure(self):
        tc = Configurator()
        tc.register(
            "MyDb", {"name": "TestDb{ID}", "path": "/mnt/spetlr/silver/testdb{ID}"}
        )

        tc.register(
            "MyTbl",
            {
                "name": "TestDb{ID}.TestTbl",
                "path": "/mnt/spetlr/silver/testdb{ID}/testtbl",
            },
        )

        tc.register(
            "MyTbl2",
            {
                "name": "TestDb{ID}.TestTbl2",
            },
        )

        tc.register(
            "MyTbl3",
            {
                "path": "/mnt/spetlr/silver/testdb{ID}/testtbl3",
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

        # test instantiation without error
        DbHandle.from_tc("MyDb")
        DeltaHandle.from_tc("MyTbl")
        DeltaHandle.from_tc("MyTbl2")
        DeltaHandle.from_tc("MyTbl3")
        DeltaHandle.from_tc("MyTbl4")
        DeltaHandle.from_tc("MyTbl5")

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

    def test_03_create(self):
        db = DbHandle.from_tc("MyDb")
        db.drop_cascade()
        db.create()

        dh = DeltaHandle.from_tc("MyTbl")
        dh.create_hive_table()

        # test hive access:
        df = dh.read()
        self.assertTrue(6, df.count())

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

    def test_07_write_path_only(self):
        # check that we can write to the table with no path
        df = DeltaHandle.from_tc("MyTbl").read()

        dh3 = DeltaHandle.from_tc("MyTbl3")

        dh3.append(df, mergeSchema=True)

        df = dh3.read()
        df.show()

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
