import unittest
from unittest.mock import MagicMock

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from atc.etl import Transformer, Orchestration
import atc.transformations as atcT
from atc.spark import Spark


class ConcatDfTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.df1 = create_df1()
        cls.df2 = create_df2()
        cls.df3 = create_df3()

    def test_concat_one_df(self):
        atcT.concat_dfs([self.df1])

    def test_concat_two_df(self):
        atcT.concat_dfs([self.df1, self.df2])

    def test_concat_three_df(self):
        atcT.concat_dfs([self.df1, self.df2, self.df3])


def create_df1():
    return Spark.get().createDataFrame(
        Spark.get().sparkContext.parallelize([
            ('1', 'Fender', 'Telecaster', '1950'),
            ('2', 'Gibson', 'Les Paul', '1959'),
            ('3', 'Ibanez', 'RG', '1987')
        ]),
        StructType([
            StructField('id', StringType()),
            StructField('brand', StringType()),
            StructField('model', StringType()),
            StructField('year', StringType()),
        ]))


def create_df2():
    return Spark.get().createDataFrame(
        Spark.get().sparkContext.parallelize([
            ('1', 'Fender', 'Stratocaster', 'Small'),
            ('2', 'Gibson', 'Les Paul Junior', 'Medium'),
            ('3', 'Ibanez', 'JPM', 'Large')
        ]),
        StructType([
            StructField('id', StringType()),
            StructField('brand', StringType()),
            StructField('model', StringType()),
            StructField('size', StringType()),
        ]))


def create_df3():
    return Spark.get().createDataFrame(
        Spark.get().sparkContext.parallelize([
            ('1', 'Fender', 'Jaguar', 'Brown'),
            ('2', 'Gibson', 'EB', 'Blue'),
            ('3', 'Ibanez', 'AE', 'Black')
        ]),
        StructType([
            StructField('id', StringType()),
            StructField('brand', StringType()),
            StructField('model', StringType()),
            StructField('color', StringType()),
        ]))
