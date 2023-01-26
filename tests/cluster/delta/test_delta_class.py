import unittest

from pyspark.sql.utils import AnalysisException

from atc import Configurator
from atc.delta import DbHandle, DeltaHandle
from atc.etl import Orchestrator
from atc.etl.extractors import SimpleExtractor
from atc.etl.extractors.schema_extractor import SchemaExtractor
from atc.etl.loaders import SimpleLoader
from atc.spark import Spark


class DeltaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()

    def test_01_configure(self):
        tc = Configurator()
        tc.register(
            "MyDb", {"name": "TestDb{ID}", "path": "/mnt/atc/silver/testdb{ID}"}
        )

        tc.register(
            "MyTbl",
            {
                "name": "TestDb{ID}.TestTbl",
                "path": "/mnt/atc/silver/testdb{ID}/testtbl",
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
                "path": "/mnt/atc/silver/testdb/testtbl3",
            },
        )

        # test instantiation without error
        DbHandle.from_tc("MyDb")
        DeltaHandle.from_tc("MyTbl")
        DeltaHandle.from_tc("MyTbl2")

    def test_02_write(self):
        dh = DeltaHandle.from_tc("MyTbl")

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
        db.create()

        dh = DeltaHandle.from_tc("MyTbl")
        dh.create_hive_table()

        # test hive access:
        df = Spark.get().table("TestDb.TestTbl")
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
        dh = DeltaHandle.from_tc("MyTbl")
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

        dh2 = DeltaHandle.from_tc("MyTbl2")
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
