from spetlr.functions import init_dbutils


def file_exists(path: str) -> bool:
    """
    Helper function to check whether a file or folder exists.

    Todo: Feel free to implement a better solution like the following:

    This implementation should be more correct:

    try:
        init_dbutils().fs.ls(path)
        return True
    except Exception as e:
        exc_info = sys.exc_info()
        if "java.io.FileNotFoundException" in str(exc_info):
            return False
        else:
            raise e


    """

    try:
        init_dbutils().fs.ls(path)
        return True
    except Exception:
        return False
