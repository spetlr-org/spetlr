import sys

from spetlr.functions import init_dbutils


def file_exists(path: str):
    """
    Helper function to check whether a file or folder exists.
    """

    try:
        init_dbutils().fs.ls(path)
        return True
    except Exception as e:
        exc_info = sys.exc_info()
        if "java.io.FileNotFoundException" in str(exc_info):
            return False
        else:
            raise e
