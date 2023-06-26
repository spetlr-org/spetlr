from spetlrtools.testing import DataframeTestCase

from spetlr.filehandle import FileHandle


class FileHandleTests(DataframeTestCase):
    def test_01_can_set_options(self):
        """
        Simple test that options attribute is set in the FileHandle
        """

        class TestClass(FileHandle):
            def __init__(
                self,
                *,
                file_location: str,
                schema_location: str,
                data_format: str,
                options: dict
            ):
                super().__init__(
                    file_location=file_location,
                    schema_location=schema_location,
                    data_format=data_format,
                    options=options,
                )

            def print(self) -> int:
                return 1

        tester = TestClass(
            file_location="test",
            schema_location="test",
            data_format="csv",
            options={"somesetting": "somevalue"},
        )

        self.assertEqual(tester._options, {"somesetting": "somevalue"})
