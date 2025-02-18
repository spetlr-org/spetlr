import unittest

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame

from spetlr import Configurator
from spetlr.cache import CachedLoader, CachedLoaderParameters
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.spark import Spark


class TestError(Exception):
    pass


class ChildCacher(CachedLoader):
    to_be_written: DataFrame
    written: DataFrame = None
    to_be_deleted: DataFrame
    deleted: DataFrame = None
    too_many_rows_was_called: bool = False
    fail_delete: bool = False

    def write_operation(self, df: DataFrame):
        DeltaHandle.from_tc("CachedTestTarget").append(df)
        return df

    def delete_operation(self, df: DataFrame) -> DataFrame:
        target_name = Configurator().table_name("CachedTestTarget")

        df.createOrReplaceTempView("to_be_deleted")
        Spark.get().sql(
            f"""
            MERGE INTO {target_name} AS t
            USING to_be_deleted AS d
            ON t.a==d.a
            WHEN MATCHED
            THEN DELETE
        """
        )
        if self.fail_delete:
            raise TestError("intended fail point")
        return df


class CachedLoaderProvisionalMarkupTests(unittest.TestCase):
    params: CachedLoaderParameters

    @classmethod
    def setUpClass(cls) -> None:
        tc = Configurator()
        tc.clear_all_configurations()
        tc.set_debug()

        tc.register("TestDb", dict(name="cache_sequence_test{ID}"))
        tc.register(
            "CachedTest",
            dict(name="{TestDb}.cachedloader_cache"),
        )
        tc.register(
            "CachedTestTarget",
            dict(name="{TestDb}.cachedloader_target"),
        )
        DbHandle.from_tc("TestDb").create()
        spark = Spark.get()
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS {CachedTest_name}
            (
                a STRING,
                rowHash INTEGER,
                loadedTime TIMESTAMP,
                deletedTime TIMESTAMP
            )
        """.format(
                **tc.get_all_details()
            )
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS {CachedTestTarget_name}
            (
                a STRING,
                payload STRING
            )
        """.format(
                **tc.get_all_details()
            )
        )

        cls.params = CachedLoaderParameters(
            cache_table_name=tc.table_name("CachedTest"),
            key_cols=[
                "a",
            ],
        )

        cls.sut = ChildCacher(cls.params)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle("TestDb").drop_cascade()

    def test_full_sequence(self):
        spark = Spark.get()
        dh = DeltaHandle.from_tc("CachedTestTarget")

        # once, a row was loaded:
        initial_data = spark.createDataFrame(
            [
                ("id1", "payload"),
            ],
            dh.read().schema,
        )
        self.sut.save(initial_data)

        # and all was good
        self.assertEqual(dh.read().count(), 1)

        # Then an error occurred and we attempted to save an empty dataframe
        df = initial_data.filter(f.lit(False))
        self.assertEqual(df.count(), 0)
        # luckily an engineer managed to stop the process before it went too far
        self.sut.fail_delete = True
        try:
            self.sut.save(df)
        except TestError:
            pass
        self.sut.fail_delete = False

        # The next day all was back to normal, and we resumed saving the correct data
        self.sut.save(initial_data)

        # now we have the correct data in the target system once more
        self.assertEqual(dh.read().count(), 1)

        # Note: This final assert failed until we introduced the provisional markup
        # step in the cached loader.
