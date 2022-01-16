from collections import OrderedDict
from typing import List, Union, Tuple, Any

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, Row, ArrayType

from atc.spark import Spark

_column_selection = Union[str, Tuple[str, List["_column_selection"]]]


class DataframeCreator:
    @classmethod
    def make_partial(
        cls, schema: StructType, columns: List[_column_selection], data: List[Tuple]
    ) -> DataFrame:
        rows = []
        if data:
            for data_row in data:
                rows.append(cls._make_row(schema, data_row, columns))
        return Spark.get().createDataFrame(rows, schema=schema)

    @classmethod
    def make(cls, schema: StructType, data: List[Tuple]) -> DataFrame:
        return Spark.get().createDataFrame(data, schema=schema)

    @classmethod
    def _make_row_array(
        cls, schema: ArrayType, data_row: List[Any], columns: List[_column_selection]
    ) -> List[Any]:
        element = schema.elementType
        if isinstance(element, ArrayType):
            return [cls._make_row_array(element, item, columns) for item in data_row]
        elif isinstance(element, StructType):
            return [cls._make_row(element, item, columns) for item in data_row]
        else:
            return data_row

    @classmethod
    def _make_row(
        cls, schema: StructType, data_row: Tuple, columns: List[_column_selection]
    ) -> Row:

        schema_dict = {f.name: f.dataType for f in schema.fields}
        in_data = dict()
        for column, item in zip(columns, data_row):
            if isinstance(column, str):
                # we are dealing with a simple column
                assert column in schema.names
                in_data[column] = item

            else:
                # we are dealing with a composite column
                # here we need to recurse so that we can set the data to a Row object
                column_name = column[0]
                column_schema_selection = column[1]
                column_schema = schema_dict[column_name]
                if isinstance(column_schema, StructType):
                    in_data[column_name] = cls._make_row(
                        column_schema, item, column_schema_selection
                    )
                else:
                    assert isinstance(column_schema, ArrayType)
                    in_data[column_name] = cls._make_row_array(
                        column_schema, item, column_schema_selection
                    )

                    # fix the order of the items in the row item while also given the row elements keys
        kwargs = OrderedDict()
        for column in schema.names:
            if column in in_data:
                kwargs[column] = in_data[column]
            else:
                kwargs[column] = None

        return Row(**kwargs)
