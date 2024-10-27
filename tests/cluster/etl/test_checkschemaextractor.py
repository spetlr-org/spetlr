from pyspark.sql import DataFrame
from spetlrtools.testing import DataframeTestCase

from spetlr.configurator import Configurator
from spetlr.delta import DbHandle, DeltaHandle
from spetlr.etl.extractors.check_schema_extractor import CheckSchemaExtractor
from spetlr.etl.loaders import SimpleLoader
from spetlr.exceptions import SchemasNotEqualException
from spetlr.schema_manager import SchemaManager
from spetlr.spark import Spark
from tests.cluster.etl import extras
from tests.cluster.etl.extras.schemas import test_base_schema


class TestCheckSchemaExtractor(DataframeTestCase):
    test_base_schema_handle: DeltaHandle = None
    test_added_col_schema_handle: DeltaHandle = None
    test_col_type_difference_schema_handle: DeltaHandle = None
    test_removed_col_schema_handle: DeltaHandle = None
    test_renamed_col_schema_handle: DeltaHandle = None

    @classmethod
    def setUpClass(cls):
        # Initialize schemas
        SchemaManager().clear_all_configurations()
        extras.initSchemaManager()

        # Initialize table configurator
        c = Configurator()
        c.clear_all_configurations()
        c.add_resource_path(extras)
        c.set_debug()

        db = DbHandle.from_tc("TestDB")
        db.create()

        # Create handles
        cls.test_base_schema_handle = DeltaHandle.from_tc("CheckBaseSchemaTable")
        cls.test_added_col_schema_handle = DeltaHandle.from_tc(
            "CheckAddedColSchemaTable"
        )
        cls.test_col_type_difference_schema_handle = DeltaHandle.from_tc(
            "CheckColTypeDifferenceSchemaTable"
        )
        cls.test_removed_col_schema_handle = DeltaHandle.from_tc(
            "CheckRemovedColSchemaTable"
        )
        cls.test_renamed_col_schema_handle = DeltaHandle.from_tc(
            "CheckRenamedColSchemaTable"
        )

        # Create base df
        base_df = create_base_dataframe()

        # Upload df
        SimpleLoader(handle=cls.test_base_schema_handle).save(base_df)
        SimpleLoader(handle=cls.test_added_col_schema_handle).save(base_df)
        SimpleLoader(handle=cls.test_col_type_difference_schema_handle).save(base_df)
        SimpleLoader(handle=cls.test_removed_col_schema_handle).save(base_df)
        SimpleLoader(handle=cls.test_renamed_col_schema_handle).save(base_df)

    def test_equal_schemas(self):
        extractor = CheckSchemaExtractor(self.test_base_schema_handle)
        extractor.read()

    def test_col_type_difference_exception(self):
        with self.assertRaises(SchemasNotEqualException):
            extractor = CheckSchemaExtractor(
                self.test_col_type_difference_schema_handle
            )
            extractor.read()

    def test_added_col_exception(self):
        with self.assertRaises(SchemasNotEqualException):
            extractor = CheckSchemaExtractor(self.test_added_col_schema_handle)
            extractor.read()

    def test_removed_col_exception(self):
        with self.assertRaises(SchemasNotEqualException):
            extractor = CheckSchemaExtractor(self.test_removed_col_schema_handle)
            extractor.read()

    def test_renamed_col_exception(self):
        with self.assertRaises(SchemasNotEqualException):
            extractor = CheckSchemaExtractor(self.test_renamed_col_schema_handle)
            extractor.read()


def create_base_dataframe() -> DataFrame:
    return Spark.get().createDataFrame(
        Spark.get().sparkContext.parallelize([(1, "1")]),
        test_base_schema,
    )
