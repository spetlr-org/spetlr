import unittest

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from spetlrtools.time import dt_utc

from spetlr import Configurator
from spetlr.cache import CachedLoader, CachedLoaderParameters
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.spark import Spark


class ChildCacher(CachedLoader):
    to_be_written: DataFrame
    written: DataFrame = None
    to_be_deleted: DataFrame
    deleted: DataFrame = None
    too_many_rows_was_called: bool = False

    def write_operation(self, df: DataFrame):
        self.to_be_written = df
        self.written = df.filter(df["b"].isin([1, 2]))
        DeltaHandle.from_tc("CachedTestTarget").append(self.written)
        return self.written.withColumn("myId", f.lit(12345))

    def delete_operation(self, df: DataFrame) -> DataFrame:
        target_name = Configurator().table_name("CachedTestTarget")

        self.to_be_deleted = df
        Spark.get().sql(f"DELETE FROM {target_name} WHERE b = 8")
        self.deleted = df.filter(df["b"] == 8)
        return self.deleted

    def too_many_rows(self) -> None:
        self.too_many_rows_was_called = True
        return


class CachedLoaderTests(unittest.TestCase):
    params: CachedLoaderParameters
    old_cache = [
        (
            "3",
            3,
            453652661,
            dt_utc(2022, 1, 1, 14),
            None,
            99,
        ),  # match
        (
            "6",
            6,
            123456789,
            dt_utc(2022, 1, 1, 14),
            None,
            99,
        ),  # mismatch
        (
            "7",
            7,
            1284583559,
            dt_utc(2022, 1, 1, 14),
            None,
            99,
        ),  # match
        (
            "8",
            8,
            123456789,
            dt_utc(2022, 1, 1, 14),
            None,
            99,
        ),  # unknown row, to be deleted
        (
            "9",
            9,
            123456789,
            dt_utc(2022, 1, 1, 14),
            None,
            99,
        ),  # unknown row, to be deleted
    ]

    new_data = [
        ("1", 1, "foo1"),  # new
        ("2", 2, "foo2"),  # new
        ("3", 3, "foo3"),  # match
        ("6", 6, "foo6"),  # mismatch
        ("7", 7, "foo7"),  # match
        ("7", 7, "foo7"),  # duplicate row will only be loaded once
    ]

    too_much_data = [
        ("11", 1, "foo1"),  # new
        ("12", 1, "foo2"),  # new
        ("13", 1, "foo3"),  # new
        ("14", 1, "foo4"),  # new
        ("15", 1, "foo5"),  # new
        ("16", 1, "foo6"),  # new
        ("17", 1, "foo7"),  # new
        ("18", 1, "foo8"),  # new
        ("19", 1, "foo9"),  # new
        ("20", 1, "foo10"),  # new
        ("21", 1, "foo11"),  # new
    ]

    @classmethod
    def setUpClass(cls) -> None:
        tc = Configurator()
        tc.clear_all_configurations()
        tc.set_debug()

        tc.register("TestDb", dict(name="test{ID}", path="/tmp/test{ID}.db"))
        tc.register(
            "CachedTest",
            dict(name="test{ID}.cachedloader_cache"),
        )
        tc.register(
            "CachedTestTarget",
            dict(name="test{ID}.cachedloader_target"),
        )
        DbHandle.from_tc("TestDb").create()
        spark = Spark.get()
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS {CachedTest_name}
            (
                a STRING,
                b INTEGER,
                rowHash INTEGER,
                loadedTime TIMESTAMP,
                deletedTime TIMESTAMP,
                myId INTEGER
            )
            USING DELTA
            COMMENT "Caching Test"
        """.format(
                **tc.get_all_details()
            )
        )

        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS {CachedTestTarget_name}
            (
                a STRING,
                b INTEGER,
                payload STRING
            )
            USING DELTA
            COMMENT "Caching target"
        """.format(
                **tc.get_all_details()
            )
        )

        cls.params = CachedLoaderParameters(
            cache_table_name=tc.table_name("CachedTest"),
            key_cols=["a", "b"],
            cache_id_cols=["myId"],
            do_nothing_if_more_rows_than=10,
        )

        cls.sut = ChildCacher(cls.params)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle("TestDb").drop_cascade()

    def test_01_can_perform_cached_write(self):
        cache_dh = DeltaHandle.from_tc("CachedTest")
        # prime the cache
        df_old_cache = Spark.get().createDataFrame(
            self.old_cache, schema=cache_dh.read().schema
        )
        cache_dh.overwrite(df_old_cache)

        # prepare the new data
        target_dh = DeltaHandle.from_tc("CachedTestTarget")
        df_new = Spark.get().createDataFrame(
            self.new_data, schema=target_dh.read().schema
        )

        # execute the system under test
        self.sut.save(df_new)

        cache = cache_dh.read().withColumn(
            "isRecent",
            (f.current_timestamp().cast("long") - f.col("loadedTime").cast("long"))
            < 100,
        )
        cache.show()

        # Section on writing

        # We expect that the rows that were requested for write were
        # - new rows 1 & 2
        # - mismatch row 6
        to_be_written_ids = {row.a for row in self.sut.to_be_written.collect()}
        self.assertEqual(to_be_written_ids, {"1", "2", "6"})
        # filtered actually written
        written_ids = {row.a for row in self.sut.written.collect()}
        self.assertEqual(written_ids, {"1", "2"})
        # verify that this agrees with the current state of the cache:
        new_cache = cache.filter(f.col("isRecent"))
        new_cache_ids = {row.a for row in new_cache.collect()}
        self.assertEqual(new_cache_ids, {"1", "2"})

        # Section on deleting
        # to be deleted were the missing rows 8 and 9
        to_be_deleted_ids = {row.a for row in self.sut.to_be_deleted.collect()}
        self.assertEqual(to_be_deleted_ids, {"8", "9"})

        # only row 8 was passed to be deleted. Check.
        del_cache = cache.filter(cache[self.sut.params.deletedTime].isNotNull())
        (del_id,) = [row.a for row in del_cache.collect()]
        self.assertEqual(del_id, "8")
        self.assertFalse(self.sut.too_many_rows_was_called)

    def test_02_checks_for_too_many_rows(self):
        self.sut.written = None
        self.sut.deleted = None

        cache_dh = DeltaHandle.from_tc("CachedTest")
        # prime the cache
        df_old_cache = Spark.get().createDataFrame(
            self.old_cache, schema=cache_dh.read().schema
        )
        cache_dh.overwrite(df_old_cache)

        # prepare the new data with too many rows
        target_dh = DeltaHandle.from_tc("CachedTestTarget")
        df_new = Spark.get().createDataFrame(
            self.too_much_data, schema=target_dh.read().schema
        )

        # execute the system under test
        self.sut.save(df_new)

        self.assertTrue(self.sut.too_many_rows_was_called)
        self.assertIsNone(self.sut.written)
        self.assertIsNone(self.sut.deleted)
