"""
Spark Models utilizing the base and mixin modules.
"""

import pyspark.sql.types as t

from spetlr.spark_catalog.base import (
    DeltaModel,
    SparkColumn,
    DeltaDatabase,
    DeltaCatalog,
)
from spetlr.spark_catalog.structures import (
    EventColumns,
    ObjectIDAndOriginColumns,
    TimeColumn,
)


# pylint: skip-file


class Alarm(DeltaModel, ObjectIDAndOriginColumns):
    """
    Model class for Alarm
    """

    AlarmCode = SparkColumn(name="AlarmCode", dtype=t.IntegerType())
    AlarmValue = SparkColumn(name="AlarmValue", dtype=t.StringType())
    AlarmCount = SparkColumn(name="AlarmCount", dtype=t.IntegerType())
    ObjectID = SparkColumn(name="ObjectID", dtype=t.StringType())
    DataOrigin = SparkColumn(name="DataOrigin", dtype=t.StringType())


class Event(DeltaModel, ObjectIDAndOriginColumns, TimeColumn, EventColumns):
    """
    Model class for Event
    """

    Time = SparkColumn(name="Time", dtype=t.TimestampType())
    EventCode = SparkColumn(name="EventCode", dtype=t.IntegerType())
    EventValue = SparkColumn(name="EventValue", dtype=t.StringType())
    Par_1 = SparkColumn(name="Par_1", dtype=t.StringType())
    Par_2 = SparkColumn(name="Par_2", dtype=t.StringType())
    Par_3 = SparkColumn(name="Par_3", dtype=t.StringType())
    Par_4 = SparkColumn(name="Par_4", dtype=t.StringType())
    Par_5 = SparkColumn(name="Par_5", dtype=t.StringType())


class ClaimsClassModel(DeltaDatabase):
    Alarm = Alarm()
    Event = Event()


class DataScienceCatalog(DeltaCatalog):
    ClaimsClassModel = ClaimsClassModel()
