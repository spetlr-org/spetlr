import uuid
from typing import List, Tuple

import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr import Configurator
from spetlr.filehandle import FileHandle
from spetlr.functions import init_dbutils
from spetlr.schema_manager import SchemaManager
from spetlr.spark import Spark


class FileHandleTests(DataframeTestCase):
    path_unique_id = uuid.uuid4().hex

    avro_source_path = f"/test/{path_unique_id}/avro_source"
    json_source_path = f"/test/{path_unique_id}/json_source"
    parquet_source_path = f"/test/{path_unique_id}/parquet_source"

    raw_source_schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField(
                "data", T.StringType(), True
            ),  # Column not used in source schema
        ]
    )

    source_schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField(
                "other_data", T.StringType(), True
            ),  # Column in source schema missing in data
        ]
    )

    @classmethod
    def setUpClass(cls) -> None:
        Configurator().clear_all_configurations()
        Configurator().set_debug()

        init_dbutils().fs.mkdirs(cls.avro_source_path)
        init_dbutils().fs.mkdirs(cls.json_source_path)
        init_dbutils().fs.mkdirs(cls.parquet_source_path)

    @classmethod
    def tearDownClass(cls) -> None:
        init_dbutils().fs.rm(cls.avro_source_path, True)
        init_dbutils().fs.rm(cls.json_source_path, True)
        init_dbutils().fs.rm(cls.parquet_source_path, True)

    def test_01_configure(self):
        sc = SchemaManager()
        sc.register_schema("source_schema", self.source_schema)

        tc = Configurator()

        # Add avro source
        tc.register(
            "avro_source",
            {
                "name": "avro_source",
                "path": f"{self.avro_source_path}/*",
                "format": "avro",
                "schema": "source_schema",
            },
        )
        # Add json source
        tc.register(
            "json_source",
            {
                "name": "json_source",
                "path": f"{self.json_source_path}/*",
                "format": "json",
                "schema": "source_schema",
            },
        )
        # Add parquet source
        tc.register(
            "parquet_source",
            {
                "name": "parquet_source",
                "path": f"{self.parquet_source_path}/*",
                "format": "parquet",
                "schema": "source_schema",
            },
        )

        # Test instantiation without error
        FileHandle.from_tc("avro_source")
        FileHandle.from_tc("json_source")
        FileHandle.from_tc("parquet_source")

        # Write test data to source
        self._add_avro_data_to_source([(1, "a", "data1"), (2, "b", "data2")])
        self._add_json_data_to_source([(1, "a", "data1"), (2, "b", "data2")])
        self._add_parquet_data_to_source([(1, "a", "data1"), (2, "b", "data2")])

    def test_02_read_avro_batch_with_schema(self):
        df_result = FileHandle.from_tc("avro_source").read()
        self.assertEqualSchema(df_result.schema, self.source_schema)
        self.assertDataframeMatches(
            df_result, expected_data=[(1, "a", None), (2, "b", None)]
        )

    def test_03_read_avro_batch_without_schema(self):
        fh = FileHandle.from_tc("avro_source")
        fh.set_schema(None)
        df_result = fh.read()
        self.assertEqualSchema(df_result.schema, self.raw_source_schema)
        self.assertDataframeMatches(
            df_result, expected_data=[(1, "a", "data1"), (2, "b", "data2")]
        )

    def test_04_read_json_batch(self):
        df_result = FileHandle.from_tc("json_source").read()
        self.assertEqualSchema(df_result.schema, self.source_schema)
        self.assertDataframeMatches(
            df_result, expected_data=[(1, "a", None), (2, "b", None)]
        )

    def test_05_read_parquet_batch(self):
        df_result = FileHandle.from_tc("parquet_source").read()
        self.assertEqualSchema(df_result.schema, self.source_schema)
        self.assertDataframeMatches(
            df_result, expected_data=[(1, "a", None), (2, "b", None)]
        )

    def _add_avro_data_to_source(self, input_data: List[Tuple[int, str, str]]):
        df = Spark.get().createDataFrame(input_data, self.raw_source_schema)

        df.write.format("avro").save(f"{self.avro_source_path}/{str(uuid.uuid4().hex)}")

    def _add_json_data_to_source(self, input_data: List[Tuple[int, str, str]]):
        df = Spark.get().createDataFrame(input_data, self.raw_source_schema)

        df.coalesce(1).write.format("json").save(
            f"{self.json_source_path}/{str(uuid.uuid4().hex)}.json"
        )

    def _add_parquet_data_to_source(self, input_data: List[Tuple[int, str, str]]):
        df = Spark.get().createDataFrame(input_data, self.raw_source_schema)

        df.coalesce(1).write.format("parquet").save(
            f"{self.parquet_source_path}/{str(uuid.uuid4().hex)}.parquet"
        )
