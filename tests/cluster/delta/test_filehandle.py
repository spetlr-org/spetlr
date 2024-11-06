import time
import unittest
import uuid
from typing import List, Tuple

from spetlr import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.etl import Orchestrator
from spetlr.etl.extractors.stream_extractor import StreamExtractor
from spetlr.etl.loaders import SimpleLoader
from spetlr.etl.loaders.stream_loader import StreamLoader
from spetlr.filehandle import FileHandle
from spetlr.functions import init_dbutils
from spetlr.spark import Spark
from spetlr.testutils.stop_test_streams import stop_test_streams
from tests.cluster.values import resourceName


@unittest.skip("TODO: Test uses mount points")
class FileHandleTests(unittest.TestCase):
    sink_checkpoint_path: str = None
    path_unique_id = uuid.uuid4().hex
    avrosource_checkpoint_path = (
        f"/mnt/{resourceName()}/silver/{resourceName()}"
        f"/avrolocation/{path_unique_id}/_checkpoint_path_avro"
    )

    avro_source_path = (
        f"/mnt/{resourceName()}/silver/{resourceName()}/"
        f"avrolocation/{path_unique_id}/AvroSource"
    )

    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()
        Configurator().set_debug()

        init_dbutils().fs.mkdirs(cls.avrosource_checkpoint_path)

        init_dbutils().fs.mkdirs(cls.avro_source_path)

    @classmethod
    def tearDownClass(cls) -> None:
        DbHandle.from_tc("MyDb").drop_cascade()

        init_dbutils().fs.rm(cls.avrosource_checkpoint_path, True)

        init_dbutils().fs.rm(cls.avro_source_path, True)

        stop_test_streams()

    def test_01_configure(self):
        tc = Configurator()
        tc.register(
            "MyDb", {"name": "TestDb{ID}", "path": "/mnt/spetlr/silver/testdb{ID}"}
        )
        # add avro source
        tc.register(
            "AvroSource",
            {
                "name": "AvroSource",
                "path": self.avro_source_path,
                "format": "avro",
                "partitioning": "ymd",
                "schema_location": self.avrosource_checkpoint_path,
            },
        )

        # Add sink table
        self.sink_checkpoint_path = (
            "/mnt/spetlr/silver/testdb{ID}/_checkpoint_path_avrosink"
        )

        # add eventhub sink
        tc.register(
            "AvroSink",
            {
                "name": "{MyDb}.AvroSink",
                "path": "{MyDb_path}/AvroSink",
                "format": "delta",
                "schema_location": self.sink_checkpoint_path,
                "await_termination": True,
                "query_name": "testquery{ID}",
            },
        )

        # test instantiation without error
        DbHandle.from_tc("MyDb")
        FileHandle.from_tc("AvroSource")
        DeltaHandle.from_tc("AvroSink")

    def test_02_read_avro(self):
        tc = Configurator()
        DbHandle.from_tc("MyDb").create()

        dh_sink = DeltaHandle.from_tc("AvroSink")
        Spark.get().sql(
            f"""
                            CREATE TABLE {dh_sink.get_tablename()}
                            (
                            id int,
                            name string,
                            _rescued_data string
                            )
                            USING DELTA
                            LOCATION '{tc.get("AvroSink","path")}'
                        """
        )

        self._add_avro_data_to_source([(1, "a", "None"), (2, "b", "None")])

        o = Orchestrator()
        o.extract_from(
            StreamExtractor(FileHandle.from_tc("AvroSource"), dataset_key="AvroSource")
        )

        o.load_into(
            StreamLoader(
                loader=SimpleLoader(dh_sink, mode="append"),
                options_dict={},
                await_termination=True,
                checkpoint_path=tc.get("AvroSink", "schema_location"),
            )
        )
        o.execute()

        result = DeltaHandle.from_tc("AvroSink").read()

        self.assertEqual(2, result.count())

        # Run again. Should not append more.
        o.execute()
        self.assertEqual(2, result.count())

        self._add_avro_data_to_source([(3, "c", "None"), (4, "d", "None")])

        # Run again. Should append.
        o.execute()
        self.assertEqual(4, result.count())

        # Add specific data to source
        self._add_specific_data_to_source()
        o.execute()
        self.assertEqual(5, result.count())

        # If the same file is altered
        # the new row is appended also
        self._alter_specific_data()
        o.execute()
        self.assertEqual(6, result.count())

    def _create_tbl_mirror(self):
        dh = DeltaHandle.from_tc("MyTblMirror")
        Spark.get().sql(
            f"""
                            CREATE TABLE {dh.get_tablename()}
                            (
                            id int,
                            name string,
                            _rescued_data string
                            )
                            LOCATION '{Configurator().get("MyTblMirror","path")}'
                        """
        )

    def _add_avro_data_to_source(self, input_data: List[Tuple[int, str, str]]):
        df = Spark.get().createDataFrame(
            input_data, "id int, name string, _rescued_data string"
        )

        path = self.avro_source_path + "/" + str(uuid.uuid4())
        df.write.format("avro").save(path)

        for _ in range(20):
            # ensure files are created.
            df = Spark.get().read.format("avro").load(path)
            if df.count() >= len(input_data):
                break
                # all data is in target
            # else continue
            time.sleep(1)
        else:
            # if the loop continues to the end, something went wrong.
            raise Exception(f"Timeout waiting for avro test data in {path}")

    def _add_specific_data_to_source(self):
        df = Spark.get().createDataFrame(
            [(10, "specific", "None")], "id int, name string, _rescued_data string"
        )

        df.write.format("avro").save(self.avro_source_path + "/specific")

    def _alter_specific_data(self):
        df = Spark.get().createDataFrame(
            [(11, "specific", "None")], "id int, name string, _rescued_data string"
        )

        df.write.format("avro").mode("overwrite").save(
            self.avro_source_path + "/specific"
        )
