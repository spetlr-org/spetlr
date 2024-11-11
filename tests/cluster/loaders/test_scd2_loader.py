import unittest
from datetime import datetime, timezone

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from spetlrtools.testing import DataframeTestCase
from spetlrtools.time import dt_utc

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.etl import Orchestrator
from spetlr.etl.extractors import StreamExtractor
from spetlr.etl.loaders import StreamLoader
from spetlr.etl.loaders.scd2_loader import SCD2UpsertLoader
from spetlr.spark import Spark
from spetlr.testutils import stop_test_streams


@unittest.skip("TODO: Test uses mount points")
class TestSCD2Loader(DataframeTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        """
        Set up configuration for all test cases.
        Clearing any existing configurations and setting up the debug mode
        for better visibility during testing.
        """
        Configurator().clear_all_configurations()
        Configurator().set_debug()

    @classmethod
    def tearDownClass(cls) -> None:
        """
        Tear down the database setup after all tests have been run.
        Ensuring a clean state for subsequent tests.
        """
        DbHandle.from_tc("MyDb").drop_cascade()
        stop_test_streams()

    def test_01_single_join_col(self):
        """
        Test the SCD2UpsertLoader functionality.
        This involves setting up a test database and table,
        loading initial data, and then validating the
        Slowly Changing Dimension Type 2 (SCD2) behavior.
        """

        self._create_table()

        dh = DeltaHandle.from_tc("MyTbl")

        """
        First test takes this input data:
            | Id |   Col1   |    Col2    |      TimeCol      |
            |---------|----------|------------|------------------|
            |    1    |  Fender  | Telecaster | 2021-07-01 10:00 |
            |    1    |  Fender  | Telecaster | 2021-07-01 10:00 |
            |    1    |  Fender  | Telecaster | 2021-07-01 11:00 |
            |    2    |  Gibson  |  Les Paul  | 2021-07-01 11:00 |
            |    3    |  Ibanez  |     RG     | 2021-08-01 11:00 |
            |    3    |  Ibanez  |     RG     | 2021-09-01 11:00 |
            |    3    |  Ibanez  |     RG     | 2021-10-01 11:00 |


        """

        data_start = [
            (1, "Fender", "Telecaster", dt_utc(2021, 7, 1, 10)),
            # Duplicate when comparing the columns
            (1, "Fender", "Telecaster", dt_utc(2021, 7, 1, 10)),
            (1, "Fender", "Telecaster", dt_utc(2021, 7, 1, 11)),
            (2, "Gibson", "Les Paul", dt_utc(2021, 7, 1, 11)),
            (3, "Ibanez", "RG", dt_utc(2021, 8, 1, 11)),
            (3, "Ibanez", "RG", dt_utc(2021, 9, 1, 11)),
            (3, "Ibanez", "RG", dt_utc(2021, 10, 1, 11)),
        ]
        schema_start = StructType(
            [
                StructField("Id", IntegerType(), True),
                StructField("Col1", StringType(), True),
                StructField("Col2", StringType(), True),
                StructField("TimeCol", TimestampType(), True),
            ]
        )
        # Create a DataFrame from the test data
        df_in = Spark.get().createDataFrame(data=data_start, schema=schema_start)

        # Define the columns for joining and time column for the SCD2 process
        _join_cols = ["Id"]
        _time_col = "TimeCol"

        SCD2UpsertLoader(sink_handle=dh, join_cols=_join_cols, time_col=_time_col).save(
            df_in
        )

        max_time = datetime(2262, 4, 11, tzinfo=timezone.utc)

        """
        And generates this:
                |PK|Col1| Col2  |      Time     | ValFrom  |  ValTo   |Curr|Hash|
                |--|----|-------|---------------|----------|---------|----|----|
                |1 |Fend|Telec. |2021-07-01 10:|2021-07-01|21-07-01 1| F  |b69x|
                |1 |Fend|Telec. |2021-07-01 11:|2021-07-01| max_time | T  |276x|
                |2 |Gibs|L.Paul|2021-07-01 11:|2021-07-01| max_time | T  |f79x|
                |3 |Iban| RG   |2021-08-01 11:|2021-08-01|21-09-01 1| F  |7d5x|
                |3 |Iban| RG   |2021-09-01 11:|2021-09-01|21-10-01 1| F  |274x|
                |3 |Iban| RG   |2021-10-01 11:|2021-10-01| max_time | T  |fb1x|
        """

        # Define the expected outcome after the initial data load
        # This includes the handling of duplicate records and
        # setting up the initial state of the SCD2 table

        expected_data = [
            (
                1,
                "Fender",
                "Telecaster",
                dt_utc(2021, 7, 1, 10),
                dt_utc(2021, 7, 1, 10),
                dt_utc(2021, 7, 1, 11),
                False,
                "0844d4ffea6ffadf1b1609efeb3cd16a",
            ),
            (
                1,
                "Fender",
                "Telecaster",
                dt_utc(2021, 7, 1, 11),
                dt_utc(2021, 7, 1, 11),
                max_time,
                True,
                "d26e66604eb633097990f29a77043ba6",
            ),
            (
                2,
                "Gibson",
                "Les Paul",
                dt_utc(2021, 7, 1, 11),
                dt_utc(2021, 7, 1, 11),
                max_time,
                True,
                "0edf5ded053216e1497eadc7106a4f9b",
            ),
            (
                3,
                "Ibanez",
                "RG",
                dt_utc(2021, 8, 1, 11),
                dt_utc(2021, 8, 1, 11),
                dt_utc(2021, 9, 1, 11),
                False,
                "13ca996bde85cf51639c897a62cc01de",
            ),
            (
                3,
                "Ibanez",
                "RG",
                dt_utc(2021, 9, 1, 11),
                dt_utc(2021, 9, 1, 11),
                dt_utc(2021, 10, 1, 11),
                False,
                "8aff38592b66182bce84144844d90f7e",
            ),
            (
                3,
                "Ibanez",
                "RG",
                dt_utc(2021, 10, 1, 11),
                dt_utc(2021, 10, 1, 11),
                max_time,
                True,
                "968701f7bb46f602862a561ea1f9e998",
            ),
        ]

        # Read the resulting DataFrame from the SCD2 table and sort it for comparison
        df_result = dh.read().sort("Id", "TimeCol")

        # Assert that the DataFrame from the SCD2 table matches the expected data
        self.assertDataframeMatches(df_result, expected_data=expected_data)

        # Additional assertions to verify the integrity and correctness
        # of the SCD2 process. These checks ensure that the 'iscurrent' flag is properly
        # managed and that the deduplication logic works as expected
        self.assertEqual(dh.read().where("iscurrent").count(), 3)
        self.assertEqual(dh.read().where("iscurrent and Id=1").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=2").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=3").count(), 1)
        self.assertEqual(dh.read().select("hashvalue").distinct().count(), 6)

        """
        Then comes some new data:

        | Id | Col1   | Col2       | TimeCol               |
        |---------|--------|------------|-----------------------|
        | 1       | Fender | Asomething | 2022-01-01 00:00:00   |
        | 1       | Fender | Another   | 2022-02-01 00:00:00   |
        | 3       | Ibanez | Anew       | 2020-07-01 10:00:00   |
        """

        # This will update the id=1 and id = 3 in the sink table
        # Also, the id=3 is a late arrival from 2020 (a year earlier than the existing)
        data_new = [
            (1, "Fender", "Asomething", dt_utc(2022, 1, 1)),
            (1, "Fender", "Another", dt_utc(2022, 2, 1)),
            (3, "Ibanez", "Anew", dt_utc(2020, 7, 1, 10)),
        ]
        df_new = Spark.get().createDataFrame(data=data_new, schema=schema_start)

        _join_cols = ["Id"]
        _time_col = "TimeCol"
        SCD2UpsertLoader(sink_handle=dh, join_cols=_join_cols, time_col=_time_col).save(
            df_new
        )

        self.assertEqual(dh.read().where("iscurrent").count(), 3)
        self.assertEqual(dh.read().where("iscurrent and Id=1").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=2").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=3").count(), 1)
        self.assertEqual(dh.read().select("hashvalue").distinct().count(), 9)

        """
        New data arrives:

        | Id | Col1       | Col2       | TimeCol               |
        |---------|------------|------------|-----------------------|
        | 4       | FancyBrand | HelloThere | 2023-01-01 00:00:00   |

        """

        # This will update the sink table data with a new id=4
        data_new = [
            (4, "FancyBrand", "HelloThere", dt_utc(2023, 1, 1)),
        ]
        df_new = Spark.get().createDataFrame(data=data_new, schema=schema_start)

        SCD2UpsertLoader(sink_handle=dh, join_cols=_join_cols, time_col=_time_col).save(
            df_new
        )

        self.assertEqual(dh.read().where("iscurrent").count(), 4)
        self.assertEqual(dh.read().where("iscurrent and Id=1").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=2").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=3").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=4").count(), 1)
        self.assertEqual(dh.read().select("hashvalue").distinct().count(), 10)

        """
        Then this data arrives:
        | Id | Col1       | Col2    | TimeCol               |
        |---------|------------|---------|-----------------------|
        | 4       | FancyBrand | Changed | 2024-01-01 00:00:00   |
        """

        # This will update the data with a id=4 again
        data_new = [
            (4, "FancyBrand", "Changed", dt_utc(2024, 1, 1)),
        ]
        df_new = Spark.get().createDataFrame(data=data_new, schema=schema_start)

        _join_cols = ["Id"]
        _time_col = "TimeCol"
        SCD2UpsertLoader(sink_handle=dh, join_cols=_join_cols, time_col=_time_col).save(
            df_new
        )

        self.assertEqual(dh.read().where("iscurrent").count(), 4)
        self.assertEqual(dh.read().where("iscurrent and Id=1").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=2").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=3").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=4").count(), 1)
        self.assertEqual(dh.read().select("hashvalue").distinct().count(), 11)

    def test_02_multiple_join_cols(self):
        """
        Test the SCD2UpsertLoader functionality.
        This involves setting up a test database and table,
        loading initial data, and then validating the
        Slowly Changing Dimension Type 2 (SCD2) behavior.
        """
        self._create_table()

        dh = DeltaHandle.from_tc("MyTbl")

        """
        First test takes this input data:
            | Id |   Col1   |    Col2    |      TimeCol      |
            |---------|----------|------------|------------------|
            |    1    |  Fender  | Telecaster | 2021-07-01 10:00 |
            |    1    |  Fender  | Telecaster | 2021-07-01 10:00 |
            |    1    |  Super  | Telecaster | 2021-07-01 11:00 |
            |    2    |  Gibson  |  Les Paul  | 2021-07-01 11:00 |
            |    3    |  Ibanez  |     RG     | 2021-08-01 11:00 |
            |    3    |  Ibanez  |     RG     | 2021-09-01 11:00 |
            |    3    |  Ibanez  |     RG     | 2021-10-01 11:00 |


        """

        data_start = [
            (1, "Fender", "Telecaster", dt_utc(2021, 7, 1, 10)),
            # Duplicate when comparing the columns
            (1, "Fender", "Telecaster", dt_utc(2021, 7, 1, 10)),
            (1, "Super", "Telecaster", dt_utc(2021, 7, 1, 11)),
            (2, "Gibson", "Les Paul", dt_utc(2021, 7, 1, 11)),
            (3, "Ibanez", "RG", dt_utc(2021, 8, 1, 11)),
            (3, "Ibanez", "RG", dt_utc(2021, 9, 1, 11)),
            (3, "Ibanez", "RG", dt_utc(2021, 10, 1, 11)),
        ]
        schema_start = StructType(
            [
                StructField("Id", IntegerType(), True),
                StructField("Col1", StringType(), True),
                StructField("Col2", StringType(), True),
                StructField("TimeCol", TimestampType(), True),
            ]
        )
        # Create a DataFrame from the test data
        df_in = Spark.get().createDataFrame(data=data_start, schema=schema_start)

        # Define the columns for joining and time column for the SCD2 process
        _join_cols = ["Id", "Col1"]
        _time_col = "TimeCol"

        SCD2UpsertLoader(sink_handle=dh, join_cols=_join_cols, time_col=_time_col).save(
            df_in
        )

        max_time = datetime(2262, 4, 11, tzinfo=timezone.utc)

        """
        And generates this:
                |PK|Col1| Col2  |      Time     | ValFrom  |  ValTo   |Curr|Hash|
                |--|----|-------|---------------|----------|---------|----|----|
                |1 |Fend|Telec. |2021-07-01 10:|2021-07-01|max_time| T  |b69x|
                |1 |Super|Telec. |2021-07-01 11:|2021-07-01| max_time | T  |276x|
                |2 |Gibs|L.Paul|2021-07-01 11:|2021-07-01| max_time | T  |f79x|
                |3 |Iban| RG   |2021-08-01 11:|2021-08-01|21-09-01 1| F  |7d5x|
                |3 |Iban| RG   |2021-09-01 11:|2021-09-01|21-10-01 1| F  |274x|
                |3 |Iban| RG   |2021-10-01 11:|2021-10-01| max_time | T  |fb1x|
        """

        # Define the expected outcome after the initial data load
        # This includes the handling of duplicate records and
        # setting up the initial state of the SCD2 table

        expected_data = [
            (
                1,
                "Fender",
                "Telecaster",
                dt_utc(2021, 7, 1, 10),
                dt_utc(2021, 7, 1, 10),
                max_time,
                True,
                "0844d4ffea6ffadf1b1609efeb3cd16a",
            ),
            (
                1,
                "Super",
                "Telecaster",
                dt_utc(2021, 7, 1, 11),
                dt_utc(2021, 7, 1, 11),
                max_time,
                True,
                "8ee439f432890edef5e529b813a91f10",
            ),
            (
                2,
                "Gibson",
                "Les Paul",
                dt_utc(2021, 7, 1, 11),
                dt_utc(2021, 7, 1, 11),
                max_time,
                True,
                "0edf5ded053216e1497eadc7106a4f9b",
            ),
            (
                3,
                "Ibanez",
                "RG",
                dt_utc(2021, 8, 1, 11),
                dt_utc(2021, 8, 1, 11),
                dt_utc(2021, 9, 1, 11),
                False,
                "13ca996bde85cf51639c897a62cc01de",
            ),
            (
                3,
                "Ibanez",
                "RG",
                dt_utc(2021, 9, 1, 11),
                dt_utc(2021, 9, 1, 11),
                dt_utc(2021, 10, 1, 11),
                False,
                "8aff38592b66182bce84144844d90f7e",
            ),
            (
                3,
                "Ibanez",
                "RG",
                dt_utc(2021, 10, 1, 11),
                dt_utc(2021, 10, 1, 11),
                max_time,
                True,
                "968701f7bb46f602862a561ea1f9e998",
            ),
        ]

        # Read the resulting DataFrame from the SCD2 table and sort it for comparison
        df_result = dh.read().sort("Id", "TimeCol")

        # Assert that the DataFrame from the SCD2 table matches the expected data
        self.assertDataframeMatches(df_result, expected_data=expected_data)

        # Additional assertions to verify the integrity and correctness
        # of the SCD2 process. These checks ensure that the 'iscurrent' flag is properly
        # managed and that the deduplication logic works as expected
        self.assertEqual(dh.read().where("iscurrent").count(), 4)
        self.assertEqual(
            dh.read().where('iscurrent and Id=1 and Col1="Fender"').count(), 1
        )
        self.assertEqual(
            dh.read().where('iscurrent and Id=1 and Col1="Super"').count(), 1
        )
        self.assertEqual(dh.read().where("iscurrent and Id=2").count(), 1)
        self.assertEqual(dh.read().where("iscurrent and Id=3").count(), 1)
        self.assertEqual(dh.read().select("hashvalue").distinct().count(), 6)

    def test_03_streaming(self):
        """
        Test the SCD2UpsertLoader functionality
        in a streaming setup.
        """
        tc = Configurator()
        tc.set_debug()

        # Register database and tables
        self._register_database()
        self._register_stream_tables()

        # Initialize handlers to point to tables
        dh_source = DeltaHandle.from_tc("MyTblStreamSource")
        dh_sink = DeltaHandle.from_tc("MyTblStreamSink")

        # Recreate database
        dbh = DbHandle.from_tc("MyDb")
        dbh.drop_cascade()  # drop from last
        dbh.create()

        data_start = [
            (1, "Fender", "Telecaster", dt_utc(2021, 7, 1, 10)),
            # Duplicate when comparing the columns
            (1, "Fender", "Telecaster", dt_utc(2021, 7, 1, 10)),
            (1, "Fender", "Telecaster", dt_utc(2021, 7, 1, 11)),
            (2, "Gibson", "Les Paul", dt_utc(2021, 7, 1, 11)),
            (3, "Ibanez", "RG", dt_utc(2021, 8, 1, 11)),
            (3, "Ibanez", "RG", dt_utc(2021, 9, 1, 11)),
            (3, "Ibanez", "RG", dt_utc(2021, 10, 1, 11)),
        ]
        schema_start = StructType(
            [
                StructField("Id", IntegerType(), True),
                StructField("Col1", StringType(), True),
                StructField("Col2", StringType(), True),
                StructField("TimeCol", TimestampType(), True),
            ]
        )
        # Create a DataFrame from the test data
        df_in = Spark.get().createDataFrame(data=data_start, schema=schema_start)

        Spark.get().sql(
            f"""
                    CREATE TABLE {dh_source.get_tablename()}
                    (
                    Id integer,
                    Col1 string,
                    Col2 string,
                    TimeCol timestamp
                    )
                """
        )

        Spark.get().sql(
            f"""
                    CREATE TABLE {dh_sink.get_tablename()}
                    (
                    Id integer,
                    Col1 string,
                    Col2 string,
                    TimeCol timestamp,
                    ValidFrom timestamp,
                    ValidTo timestamp,
                    IsCurrent boolean,
                    HashValue string
                    )
                """
        )

        # Define the columns for joining and time column for the SCD2 process
        _join_cols = ["Id"]
        _time_col = "TimeCol"

        dh_source.overwrite(df_in)
        self.assertEqual(dh_source.read().count(), 7)

        o = Orchestrator()
        o.extract_from(StreamExtractor(dh_source, dataset_key="MyTblSource"))
        o.load_into(
            StreamLoader(
                loader=SCD2UpsertLoader(
                    dh_sink, join_cols=_join_cols, time_col=_time_col
                ),
                await_termination=True,
                checkpoint_path=Configurator().get(
                    "MyTblStreamSink", "checkpoint_path"
                ),
                query_name=Configurator().get("MyTblStreamSink", "query_name"),
            )
        )
        o.execute()

        max_time = datetime(2262, 4, 11, tzinfo=timezone.utc)

        """
        And generates this:
                |PK|Col1| Col2  |      Time     | ValFrom  |  ValTo   |Curr|Hash|
                |--|----|-------|---------------|----------|---------|----|----|
                |1 |Fend|Telec. |2021-07-01 10:|2021-07-01|21-07-01 1| F  |b69x|
                |1 |Fend|Telec. |2021-07-01 11:|2021-07-01| max_time | T  |276x|
                |2 |Gibs|L.Paul|2021-07-01 11:|2021-07-01| max_time | T  |f79x|
                |3 |Iban| RG   |2021-08-01 11:|2021-08-01|21-09-01 1| F  |7d5x|
                |3 |Iban| RG   |2021-09-01 11:|2021-09-01|21-10-01 1| F  |274x|
                |3 |Iban| RG   |2021-10-01 11:|2021-10-01| max_time | T  |fb1x|
        """

        # Define the expected outcome after the initial data load
        # This includes the handling of duplicate records and
        # setting up the initial state of the SCD2 table

        expected_data = [
            (
                1,
                "Fender",
                "Telecaster",
                dt_utc(2021, 7, 1, 10),
                dt_utc(2021, 7, 1, 10),
                dt_utc(2021, 7, 1, 11),
                False,
                "0844d4ffea6ffadf1b1609efeb3cd16a",
            ),
            (
                1,
                "Fender",
                "Telecaster",
                dt_utc(2021, 7, 1, 11),
                dt_utc(2021, 7, 1, 11),
                max_time,
                True,
                "d26e66604eb633097990f29a77043ba6",
            ),
            (
                2,
                "Gibson",
                "Les Paul",
                dt_utc(2021, 7, 1, 11),
                dt_utc(2021, 7, 1, 11),
                max_time,
                True,
                "0edf5ded053216e1497eadc7106a4f9b",
            ),
            (
                3,
                "Ibanez",
                "RG",
                dt_utc(2021, 8, 1, 11),
                dt_utc(2021, 8, 1, 11),
                dt_utc(2021, 9, 1, 11),
                False,
                "13ca996bde85cf51639c897a62cc01de",
            ),
            (
                3,
                "Ibanez",
                "RG",
                dt_utc(2021, 9, 1, 11),
                dt_utc(2021, 9, 1, 11),
                dt_utc(2021, 10, 1, 11),
                False,
                "8aff38592b66182bce84144844d90f7e",
            ),
            (
                3,
                "Ibanez",
                "RG",
                dt_utc(2021, 10, 1, 11),
                dt_utc(2021, 10, 1, 11),
                max_time,
                True,
                "968701f7bb46f602862a561ea1f9e998",
            ),
        ]

        # Read the resulting DataFrame from the SCD2 table and sort it for comparison
        df_result = dh_sink.read().sort("Id", "TimeCol")

        # Assert that the DataFrame from the SCD2 table matches the expected data
        self.assertDataframeMatches(df_result, expected_data=expected_data)

        # Additional assertions to verify the integrity and correctness
        # of the SCD2 process. These checks ensure that the 'iscurrent' flag is properly
        # managed and that the deduplication logic works as expected
        self.assertEqual(dh_sink.read().where("iscurrent").count(), 3)
        self.assertEqual(dh_sink.read().where("iscurrent and Id=1").count(), 1)
        self.assertEqual(dh_sink.read().where("iscurrent and Id=2").count(), 1)
        self.assertEqual(dh_sink.read().where("iscurrent and Id=3").count(), 1)
        self.assertEqual(dh_sink.read().select("hashvalue").distinct().count(), 6)

    def _create_table(self):
        # Set up test configurations for database and table
        tc = Configurator()
        tc.set_debug()
        self._register_database()

        tc.register(
            "MyTbl",
            {
                "name": "TestDb{ID}.TestTbl",
                "path": "/mnt/spetlr/silver/testdb{ID}/testtbl",
                "format": "delta",
            },
        )

        # Initialize and create the database and table for testing
        dbh = DbHandle.from_tc("MyDb")
        dbh.drop_cascade()  # drop from last
        dbh.create()
        dh = DeltaHandle.from_tc("MyTbl")

        Spark.get().sql(
            f"""
                    CREATE TABLE {dh.get_tablename()}
                    (
                    Id integer,
                    Col1 string,
                    Col2 string,
                    TimeCol timestamp,
                    ValidFrom timestamp,
                    ValidTo timestamp,
                    IsCurrent boolean,
                    HashValue string
                    )
                """
        )

        """
        First test takes this input data:
            | Id |   Col1   |    Col2    |      TimeCol      |
            |---------|----------|------------|------------------|
            |    1    |  Fender  | Telecaster | 2021-07-01 10:00 |
            |    1    |  Fender  | Telecaster | 2021-07-01 10:00 |
            |    1    |  Super  | Telecaster | 2021-07-01 11:00 |
            |    2    |  Gibson  |  Les Paul  | 2021-07-01 11:00 |
            |    3    |  Ibanez  |     RG     | 2021-08-01 11:00 |
            |    3    |  Ibanez  |     RG     | 2021-09-01 11:00 |
            |    3    |  Ibanez  |     RG     | 2021-10-01 11:00 |


        """

    def _register_database(self):
        tc = Configurator()
        tc.set_debug()
        tc.register(
            "MyDb", {"name": "TestDb{ID}", "path": "/mnt/spetlr/silver/testdb{ID}"}
        )

    def _register_stream_tables(self):
        tc = Configurator()
        tc.set_debug()
        tc.register(
            "MyTblStreamSource",
            {
                "name": "TestDb{ID}.MyTblStreamSource",
                "path": "/mnt/spetlr/silver/testdb{ID}/testtblstreamsource",
                "format": "delta",
                "checkpoint_path": "/mnt/spetlr/silver/testdb{ID}/"
                "_checkpoint_path_tblstreamsource",
                "query_name": "testquerytblstreamsource{ID}",
            },
        )

        tc.register(
            "MyTblStreamSink",
            {
                "name": "TestDb{ID}.TestTblStreamSink",
                "path": "/mnt/spetlr/silver/testdb{ID}/testtblstreamsink",
                "format": "delta",
                "checkpoint_path": "/mnt/spetlr/silver/testdb{ID}/"
                "_checkpoint_path_tblstreamsink",
                "query_name": "testquerytblstreamsink{ID}",
            },
        )
