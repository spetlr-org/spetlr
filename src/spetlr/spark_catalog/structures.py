"""
Mixin classes for Spark table, schema and database creation
"""

from pyspark.sql import types as t

from models.environment.spark_api.base import SparkColumn, DeltaModelStructure


# pylint: disable=too-few-public-methods
class ObjectIDAndOriginColumns(DeltaModelStructure):
    """
    Mixin class for injecting ObjectID and DataOrigin columns
    """

    ObjectID = SparkColumn(name="ObjectID", dtype=t.StringType())
    DataOrigin = SparkColumn(name="DataOrigin", dtype=t.StringType())


class TimeColumn(DeltaModelStructure):
    """
    Mixin class for injecting Time column into a table.
    """

    Time = SparkColumn(name="Time", dtype=t.TimestampType())


class EventColumns(DeltaModelStructure):
    """
    Mixin class for injecting EventCode and StepID columns into a table.
    """

    EventCode = SparkColumn(name="EventCode", dtype=t.IntegerType())
    EventValue = SparkColumn(name="EventValue", dtype=t.StringType())
