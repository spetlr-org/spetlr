"""
A collection of useful pyspark snippets.
"""

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid as _uuid

# Pyspark uuid function implemented as recommended here
# https://stackoverflow.com/questions/49785108/spark-streaming-with-python-how-to-add-a-uuid-column/50095755
uuid = udf(lambda: str(_uuid.uuid4()), StringType()).asNondeterministic()  # noqa
