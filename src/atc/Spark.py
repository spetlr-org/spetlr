"""
Obtain a singleton spark instance.
Add configuration options using the method Spark.configure.
Obtain the instance by calling Spark.get()
Some standard options are pre-set, call configure with value=None to remove them.
"""

# This class uses module level singleton pattern as suggested by method5 of
# https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python



from typing import Optional

from pyspark.sql import SparkSession

_spark: Optional[SparkSession] = None
_configurations = {
    "spark.sql.autoBroadcastJoinThreshold": -1,
    "spark.sql.session.timeZone": "Etc/UTC",
    "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
    "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
}


def config(key, value=None) -> None:
    """
    :param key: The spark configuration key
    :param value: The value to set. If the value is missing or None, the configurarion will be removed
    :return:
    """
    if _spark is not None:
        raise Exception("Configuration method called after spark session build.")
    if value is not None:
        _configurations[key] = value
    else:
        if key in _configurations:
            del _configurations[key]


def get() -> SparkSession:
    """
    :return:
    The current spark session.
    """
    global _spark
    if _spark is not None:
        return _spark
    builder = SparkSession.builder
    for key, value in _configurations.items():
        builder = builder.config(key, value)
    _spark = builder.getOrCreate()
    return _spark
