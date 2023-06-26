import uuid

from spetlrtools.testing import DataframeTestCase

from spetlr.dbutils import file_exists
from spetlr.functions import init_dbutils
from spetlr.spark import Spark


class TestFileExists(DataframeTestCase):
    """
    This test class tests the file_exists function.
    """

    def test_file_exists_true(self):
        """Tests that the function returns True when
        a folder exists
        and
        a file in a folder exists
        """
        # Create uuid folder in tmp
        dbutils = init_dbutils()
        folder_uuid = uuid.uuid4().hex
        folder_path = f"tmp/{folder_uuid}"
        dbutils.fs.mkdirs(folder_path)

        # Check that folder exists
        self.assertTrue(file_exists(folder_path))

        df = Spark.get().createDataFrame(data=[("Alice", 1)], schema=["name", "age"])
        file_path = f"{folder_path}/test_file.json"
        df.write.json(file_path)

        # Check that file exists (it should)
        self.assertTrue(file_exists(file_path))

        # cleanup
        dbutils.fs.rm(folder_path, True)

    def test_file_exists_false(self):
        """Tests that the function returns False when
        a folder that does not exist is searched for.
        """
        # Create folder name
        # without creating the actual folder
        folder_uuid = uuid.uuid4().hex
        folder_name = f"tmp/{folder_uuid}"

        # Check that file exists (it should not)
        self.assertFalse(file_exists(folder_name))
