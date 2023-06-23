from spetlr.functions import init_dbutils


def file_exists(path: str):
    """
    Helper function to check whether a file or folder exists.
    """

    try:
        init_dbutils().fs.ls(path)
        return True
    except Exception as e:
        print("File exists Exception string:")
        print("-" * 10)
        print(str(e))
        print("-" * 10)
        if "java.io.FileNotFoundException" in str(e):
            return False
        else:
            raise e
