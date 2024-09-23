import unittest
import uuid

import pyspark.sql.types as T
from spetlrtools.testing import DataframeTestCase

from spetlr import Configurator
from spetlr.delta.delta_handle import DeltaHandleInvalidFormat
from spetlr.filehandle import FileHandle
from spetlr.schema_manager import SchemaManager


class FileHandleTests(DataframeTestCase):
    path_unique_id = uuid.uuid4().hex

    data_format = "json"
    file_location = f"file_location/{path_unique_id}/"
    checkpoint_path = f"checkpoint_location/{path_unique_id}/"
    schema_location = f"schema_location/{path_unique_id}/"

    options = {"test_option": "test_value"}

    cloud_files_options = {
        "cloudFiles.format": data_format,
        "cloudFiles.schemaLocation": schema_location,
    }

    schema = T.StructType(
        [
            T.StructField("id", T.IntegerType(), True),
            T.StructField("name", T.StringType(), True),
        ]
    )

    @classmethod
    def setUpClass(cls) -> None:
        sc = SchemaManager()
        sc.clear_all_configurations()

        sc.register_schema("SCHEMA", cls.schema)

        tc = Configurator()
        tc.clear_all_configurations()

        tc.register(
            "FileSource",
            {
                "name": "TestFileSource",
                "path": cls.file_location,
                "format": cls.data_format,
                "schema_location": cls.schema_location,
                "schema": "SCHEMA",
            },
        )

    def test_01_create_file_handle(self):
        handle = FileHandle(
            file_location=self.file_location,
            schema_location=self.schema_location,
            data_format=self.data_format,
            options=self.options,
            schema=self.schema,
        )

        self.assertEquals(handle._location, self.file_location)
        self.assertEquals(handle._data_format, self.data_format)
        self.assertEquals(handle._schema_location, self.schema_location)
        self.assertEquals(handle._options, {**self.options, **self.cloud_files_options})
        self.assertEqualSchema(handle._schema, self.schema)

    def test_02_create_file_handle_from_tc(self):
        handle = FileHandle.from_tc("FileSource")

        self.assertEquals(handle._location, self.file_location)
        self.assertEquals(handle._data_format, self.data_format)
        self.assertEquals(handle._schema_location, self.schema_location)
        self.assertEquals(handle._options, self.cloud_files_options)
        self.assertEqualSchema(handle._schema, self.schema)

    def test_03_validate_wrong_format_exception(self):
        with self.assertRaises(DeltaHandleInvalidFormat):
            FileHandle(
                file_location=self.file_location,
                schema_location=self.schema_location,
                data_format="delta",
                options=self.options,
                schema=self.schema,
            )


if __name__ == "__main__":
    unittest.main()
