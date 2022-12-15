"""
Obtain a singleton spark instance.
Add configuration options using the method Spark.configure.
Obtain the instance by calling Spark.get()
Some standard options are pre-set, call configure with value=None to remove them.
"""

# This class uses module level singleton pattern as suggested by method5 of
# https://stackoverflow.com/questions/6760685/creating-a-singleton-in-python


from typing import Optional, Tuple

from pyspark.sql import SparkSession


class Spark:
    """
    A class singleton to get a valid Spark session
    """

    _spark: Optional[SparkSession] = None
    _configurations = {
        "spark.sql.autoBroadcastJoinThreshold": -1,
        "spark.sql.session.timeZone": "Etc/UTC",
        "spark.driver.extraJavaOptions": "-Duser.timezone=UTC",
        "spark.executor.extraJavaOptions": "-Duser.timezone=UTC",
    }
    _master = None

    @classmethod
    def master(cls, master: str):
        cls._master = master
        return cls

    @classmethod
    def config(cls, key, value=None) -> None:
        """
        :param key: The spark configuration key
        :param value: The value to set.
            If the value is missing or None, the configurarion will be removed
        :return:
        """
        if cls._spark is not None:
            raise Exception("Configuration method called after spark session build.")
        if value is not None:
            cls._configurations[key] = value
        else:
            if key in cls._configurations:
                del cls._configurations[key]
        return cls

    @classmethod
    def get(cls) -> SparkSession:
        """
        :return:
        The current spark session.
        """
        if cls._spark is not None:
            return cls._spark
        builder = SparkSession.builder
        if cls._master is not None:
            builder = builder.master(cls._master)
        for key, value in cls._configurations.items():
            builder = builder.config(key, value)
        cls._spark = builder.getOrCreate()
        return cls._spark

    @classmethod
    def version(cls) -> Tuple:
        return tuple(int(p) for p in cls.get().version.split("."))

    DATABRICKS_RUNTIME_9_1 = (3, 1, 2)
