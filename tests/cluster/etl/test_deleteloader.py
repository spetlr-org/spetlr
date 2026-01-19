from spetlrtools.testing import DataframeTestCase

from spetlr import Configurator
from spetlr.delta import DeltaHandle
from spetlr.etl.loaders import DeleteLoader
from spetlr.spark import Spark


class DeleteLoaderTests(DataframeTestCase):
    dh: DeltaHandle

    @classmethod
    def setUpClass(cls) -> None:
        c = Configurator()
        c.clear_all_configurations()
        c.set_debug()

        c.register("target", {"name": "deletion_target{ID}"})
        Spark.get().sql(
            """CREATE TABLE {target}
        (
            a int,
            b int,
            s string
        )
        """.format(
                **c.get_all_details()
            )
        )
        cls.dh = DeltaHandle.from_tc("target")

    @classmethod
    def tearDownClass(cls) -> None:
        cls.dh.drop()

    def prepare_target(self):
        df = Spark.get().createDataFrame(
            [
                (1, 2, "a"),
                (2, 3, "b"),
                (4, 5, "c"),
            ],
            self.dh.read().schema,
        )
        self.dh.overwrite(df)

    def test_01_delete(self):
        """Delete rows from the target using all columns in the incoming dataframe"""
        self.prepare_target()

        loader = DeleteLoader(self.dh)
        df = Spark.get().createDataFrame([(3,), (5,)], "b int")

        loader.save(df)

        self.assertDataframeMatches(self.dh.read(), None, [(1, 2, "a")])

    def test_02_delete_join_cols(self):
        """Delete rows from the target using a subset of
        columns in the incoming dataframe"""
        self.prepare_target()

        loader = DeleteLoader(self.dh, join_cols=["a", "b"])
        df = Spark.get().createDataFrame(
            [(2, 3, "other"), (4, 5, "data")], "a int, b int, note string"
        )

        loader.save(df)

        self.assertDataframeMatches(self.dh.read(), None, [(1, 2, "a")])
