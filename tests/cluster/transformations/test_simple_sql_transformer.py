from spetlrtools.testing import DataframeTestCase

from spetlr import Configurator
from spetlr.delta import DeltaHandle
from spetlr.etl import Orchestrator
from spetlr.etl.extractors import SimpleExtractor
from spetlr.etl.loaders import SimpleLoader
from spetlr.etl.transformers.SimpleSqlTransformer import SimpleSqlTransformer
from spetlr.spark import Spark
from tests.cluster.transformations import sql


class SimpleSqlTransformerTest(DataframeTestCase):
    @classmethod
    def setUpClass(cls):
        c = Configurator()
        c.clear_all_configurations()
        c.set_debug()
        c.register("db", {"name": "SimpleSqlTransformerTestDb{ID}"})
        c.register("tbl1", {"name": "{db}.tbl1"})
        c.register("tbl2", {"name": "{db}.tbl2"})
        c.register("tbl3", {"name": "{db}.tbl3"})

        db = c.get("db", "name")
        tbl1 = c.get("tbl1", "name")
        tbl2 = c.get("tbl2", "name")
        tbl3 = c.get("tbl3", "name")

        spark = Spark.get()
        spark.sql(f"CREATE DATABASE {db};")
        spark.sql(f"""CREATE TABLE {tbl1} (a int, b int) USING DELTA;""")
        spark.sql(f"""CREATE TABLE {tbl2} (a int, c int) USING DELTA;""")
        spark.sql(f"""CREATE TABLE {tbl3} (a int, b int, c int) USING DELTA;""")

        spark.createDataFrame([(1, 2)], "a int, b int").write.mode(
            "overwrite"
        ).saveAsTable(tbl1)
        spark.createDataFrame([(1, 3)], "a int, c int").write.mode(
            "overwrite"
        ).saveAsTable(tbl2)

    @classmethod
    def tearDownClass(cls) -> None:
        Spark.get().sql(f"""DROP DATABASE {Configurator().get('db','name')} CASCADE""")

    def test_all(self):
        o = Orchestrator()
        o.extract_from(
            SimpleExtractor(DeltaHandle.from_tc("tbl1"), dataset_key="FirstTable")
        )
        o.extract_from(
            SimpleExtractor(DeltaHandle.from_tc("tbl2"), dataset_key="SecondTable")
        )
        o.transform_with(
            SimpleSqlTransformer(
                sql_modue=sql,
                sql_file_pattern="transform",
                dataset_input_keys=["FirstTable", "SecondTable"],
                dataset_output_key="SimpleSqlTransformerTestResult",
            )
        )
        o.load_into(SimpleLoader(DeltaHandle.from_tc("tbl3")))
        o.execute()

        self.assertDataframeMatches(
            DeltaHandle.from_tc("tbl3").read(), None, [(1, 2, 3)]
        )
