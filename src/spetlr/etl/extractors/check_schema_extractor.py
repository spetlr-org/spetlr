from pyspark.sql import DataFrame

from spetlr.etl import Extractor
from spetlr.exceptions import SchemasNotEqualException
from spetlr.tables.TableHandle import TableHandle


class CheckSchemaExtractor(Extractor):
    """This extractor will read from any TableHandle and
    test the read dataframe against a schema stored in the object.
    The schemas are compared for changes.
    """

    def __init__(self, handle: TableHandle, dataset_key: str = None):
        super().__init__(dataset_key=dataset_key)
        self.handle = handle

    def read(self) -> DataFrame:
        # get expected schema from handle
        expected_schema = self.handle.get_schema()
        # remove schema from handle to not use it for reading data
        self.handle.set_schema(None)
        # read data without schema from code
        data_schema = self.handle.read().schema
        # set schema back in handler
        self.handle.set_schema(expected_schema)

        if not data_schema.__eq__(expected_schema):
            raise SchemasNotEqualException(
                f"""Schemas have different number of columns.
                Data schema:
                {data_schema.json()}
                Expected schema:
                {expected_schema.json()}
                """
            )
