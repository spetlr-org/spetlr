"""
A collection of useful pyspark snippets.
"""

import uuid as _uuid

from deprecated import deprecated
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from atc.exceptions import NoDbUtils, NoTableException
from atc.spark import Spark

# Pyspark uuid function implemented as recommended here
# https://stackoverflow.com/questions/49785108/spark-streaming-with-python-how-to-add-a-uuid-column/50095755
uuid = udf(lambda: str(_uuid.uuid4()), StringType()).asNondeterministic()  # noqa


def init_dbutils():
    try:
        from pyspark.dbutils import DBUtils

        return DBUtils(Spark.get())
    except (ImportError, ModuleNotFoundError):
        try:
            import IPython

            return IPython.get_ipython().user_ns["dbutils"]
        except (ImportError, ModuleNotFoundError):
            raise NoDbUtils()


@deprecated(reason="Use DeltaHandle.drop_and_delete")
def drop_table_cascade(DBDotTableName: str) -> None:
    """
    drop_table_cascade drops a databricks database table
    and remove the files permanently

    :param DBDotTableName: db.tablename

    return None

    """

    # Check if table exists
    if not Spark.get()._jsparkSession.catalog().tableExists(DBDotTableName):
        raise NoTableException(
            f"The table {DBDotTableName} not found. Remember syntax db.tablename."
        )

    # Get table path
    table_path = str(
        Spark.get().sql(f"DESCRIBE DETAIL {DBDotTableName}").collect()[0]["location"]
    )

    if table_path is None:
        raise NoTableException("Table path is NONE.")

    # Remove files associated with table
    init_dbutils().fs.rm(table_path, recurse=True)

    # Remove table
    Spark.get().sql(f"DROP TABLE IF EXISTS {DBDotTableName}")


def get_unique_tempview_name() -> str:
    unique_id = _uuid.uuid4().hex
    temp_view_name = f"source_{unique_id}"
    return temp_view_name
