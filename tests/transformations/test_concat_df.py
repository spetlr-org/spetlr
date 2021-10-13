import unittest
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType
import atc.transformations as atc_transform
from atc.spark import Spark


class ConcatDfTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.df1 = create_df1()
        cls.df2 = create_df2()
        cls.df3 = create_df3()

    def test_concat_one_df(self):
        result = atc_transform.concat_dfs([self.df1])

        # Test that the columns are there
        cols = result.columns
        self.assertIn("id", cols)
        self.assertIn("brand", cols)
        self.assertIn("model", cols)
        self.assertIn("year", cols)

        # Test the number of rows
        self.assertEqual(3, result.count())
        self.assertEqual(1, get_number_rows_1(result, '1', 'Fender', 'Telecaster', year='1950'))
        self.assertEqual(1, get_number_rows_1(result, '2', 'Gibson', 'Les Paul', year='1959'))
        self.assertEqual(1, get_number_rows_1(result, '3', 'Ibanez', 'RG', year='1987'))

        # Test that there is only one of each column
        self.assertEqual(1, cols.count("id"))
        self.assertEqual(1, cols.count("brand"))
        self.assertEqual(1, cols.count("model"))
        self.assertEqual(1, cols.count("year"))

    def test_concat_two_df(self):
        result = atc_transform.concat_dfs([self.df1, self.df2])

        # Test that the columns are there
        cols = result.columns
        self.assertIn("id", cols)
        self.assertIn("brand", cols)
        self.assertIn("model", cols)
        self.assertIn("year", cols)
        self.assertIn("size", cols)

        # Test the number of rows
        self.assertEqual(6, result.count())
        self.assertEqual(1, get_number_rows_2(result, '1', 'Fender', 'Telecaster', year='1950'))
        self.assertEqual(1, get_number_rows_2(result, '2', 'Gibson', 'Les Paul', year='1959'))
        self.assertEqual(1, get_number_rows_2(result, '3', 'Ibanez', 'RG', year='1987'))
        self.assertEqual(1, get_number_rows_2(result, '1', 'Fender', 'Stratocaster', size='Small'))
        self.assertEqual(1, get_number_rows_2(result, '2', 'Gibson', 'Les Paul Junior', size='Medium'))
        self.assertEqual(1, get_number_rows_2(result, '3', 'Ibanez', 'JPM', size='Large'))

        # Test that there is only one of each column
        self.assertEqual(1, cols.count("id"))
        self.assertEqual(1, cols.count("brand"))
        self.assertEqual(1, cols.count("model"))
        self.assertEqual(1, cols.count("year"))
        self.assertEqual(1, cols.count("size"))

    def test_concat_three_df(self):
        result = atc_transform.concat_dfs([self.df1, self.df2, self.df3])

        # Test that the columns are there
        cols = result.columns
        self.assertIn("id", cols)
        self.assertIn("brand", cols)
        self.assertIn("model", cols)
        self.assertIn("year", cols)
        self.assertIn("size", cols)
        self.assertIn("color", cols)

        # Test the number of rows
        self.assertEqual(9, result.count())
        self.assertEqual(1, get_number_rows_3(result, '1', 'Fender', 'Telecaster', year='1950'))
        self.assertEqual(1, get_number_rows_3(result, '2', 'Gibson', 'Les Paul', year='1959'))
        self.assertEqual(1, get_number_rows_3(result, '3', 'Ibanez', 'RG', year='1987'))
        self.assertEqual(1, get_number_rows_3(result, '1', 'Fender', 'Stratocaster', size='Small'))
        self.assertEqual(1, get_number_rows_3(result, '2', 'Gibson', 'Les Paul Junior', size='Medium'))
        self.assertEqual(1, get_number_rows_3(result, '3', 'Ibanez', 'JPM', size='Large'))
        self.assertEqual(1, get_number_rows_3(result, '1', 'Fender', 'Jaguar', color='Brown'))
        self.assertEqual(1, get_number_rows_3(result, '2', 'Gibson', 'EB', color='Blue'))
        self.assertEqual(1, get_number_rows_3(result, '3', 'Ibanez', 'AE', color='Black'))

        # Test that there is only one of each column
        self.assertEqual(1, cols.count("id"))
        self.assertEqual(1, cols.count("brand"))
        self.assertEqual(1, cols.count("model"))
        self.assertEqual(1, cols.count("year"))
        self.assertEqual(1, cols.count("size"))
        self.assertEqual(1, cols.count("color"))


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


def get_number_rows_1(df, id, brand, model, year=None):
    # When testing the first transformation, theres only "year" as column
    if year is not None:
        return df.filter((f.col("id") == id) & (f.col("brand") == brand) & (f.col("model") == model) & (
            f.col("year") == year)).count()
    return None


def get_number_rows_2(df, id, brand, model, year=None, size=None):
    # When testing the second transformation, theres only "year" and "size" as column
    if year is not None:
        return df.filter((f.col("id") == id) & (f.col("brand") == brand) & (f.col("model") == model) & (
            f.col("year") == year) & (f.col("size").isNull())).count()
    if size is not None:
        return df.filter((f.col("id") == id) & (f.col("brand") == brand) & (f.col("model") == model) & (
            f.col("year").isNull()) & (f.col("size") == size)).count()

    return None


def get_number_rows_3(df, id, brand, model, year=None, size=None, color=None):
    if year is not None:
        return df.filter((f.col("id") == id) & (f.col("brand") == brand) & (f.col("model") == model) & (
            f.col("year") == year) & (f.col("size").isNull()) & (f.col("color").isNull())).count()
    if size is not None:
        return df.filter((f.col("id") == id) & (f.col("brand") == brand) & (f.col("model") == model) & (
            f.col("year").isNull()) & (f.col("size") == size) & (f.col("color").isNull())).count()
    if color is not None:
        return df.filter((f.col("id") == id) & (f.col("brand") == brand) & (f.col("model") == model) & (
            f.col("year").isNull()) & (f.col("size").isNull()) & (f.col("color") == color)).count()

    return None
