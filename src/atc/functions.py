"""
A collection of useful pyspark snippets.
"""

from atc.spark import Spark
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid as _uuid
import pyspark.sql.functions as F
from pathlib import Path

# Pyspark uuid function implemented as recommended here
# https://stackoverflow.com/questions/49785108/spark-streaming-with-python-how-to-add-a-uuid-column/50095755
uuid = udf(lambda: str(_uuid.uuid4()), StringType()).asNondeterministic()  # noqa

def init_dbutils(spark):
    try:
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
    except ImportError:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
    return dbutils

def drop_table_cascade(DataBaseName,TableName):

    #Check if table exists 
    if Spark.get()._jsparkSession.catalog().tableExists(DataBaseName, TableName):
        # #Collect file path
        # file_name = spark.read.table(f"{DataBaseName}.{TableName}").select(F.input_file_name()).take(1)
        # #Collect parent 
        # table_path=str(Path(file_name).parent)

        table_path=str(Spark.get().sql(f"DESCRIBE DETAIL {DataBaseName}.{TableName}").collect()[0]["location"])

        if table_path is None:
            raise Exception(f"Table path is NONE.")
         
        Spark.get().sql(f"DROP TABLE IF EXISTS {DataBaseName}.{TableName}") # Remove hive table        
        init_dbutils(Spark.get()).fs.rm(table_path, recurse=True)           # Remove files in table

    else:
        raise Exception(f"The table {DataBaseName}.{TableName} not found.")
