# pylint: skip-file
import re
import warnings
from abc import ABC
from typing import Union

import pyspark
import pyspark.sql.types as t

from spetlr.spark_catalog.exceptions import (
    CatalogDoesNotExists,
    CatalogException,
    DatabaseException,
    DataModelException,
)

UNITY_CATALOG_NAMING_SEPARATOR = "."


def convert_pascal_casing_to_snake_casing(pascal_cased_str: str):
    """
    Converts a pascal_cased_str to snake casing
    """
    res_list = [s for s in re.split("([A-Z][^A-Z]*)", pascal_cased_str) if s]
    res_list = [x.lower() for x in res_list]
    return "_".join(res_list)


class SparkColumn:
    """
    Data container intended to reflect a spark column.
    """

    def __init__(self, name: str, dtype: pyspark.sql.dtypes):
        self._name = name
        self._dtype = dtype

    @property
    def name(self) -> str:
        """
        Returns the name of the SparkColumn
        """
        return self._name

    @property
    def dtype(self) -> pyspark.sql.dtypes:
        """
        Returns the data type of the SparkColumn.
        """
        return self._dtype


class DeltaModelStructure(ABC):
    """
    Meta-class for standard column / table functionality.
    """

    def schema(self) -> t.StructType:
        """
        Returns the schema as a pyspark.sql.types.StructType.
        """
        fields = []
        for key, value in self.__dict__.items():
            if key.startswith("_") == True:
                continue
            if isinstance(value, DeltaModel):
                fields = fields + [t.StructField(key, value.schema(), True)]
            else:
                # print(type(value).__name__)
                if type(value).__name__ == "tuple":
                    fields = fields + [
                        t.StructField(key, value[0], True, {"comment": value[1]})
                    ]
                else:
                    fields = fields + [t.StructField(key, value, True)]

        return t.StructType(fields)


class DeltaCatalog(ABC):
    """
    Reflect Unity Catalog instance on Databricks
    """

    def __init__(self, path: str, env: str):
        # should better reflect at a later stage.
        # https://docs.databricks.com/api/workspace/catalogs/create
        self._path = path
        self._env = env
        print(self.name())

    def register_all_databases(self):
        """
        Registers all database attached as class or object attributes.
        """
        for key, value in self.__dict__.items():
            # skip hidden instance attributes
            if key.startswith("_"):
                continue
            if value is not None and isinstance(key, DeltaDatabase):
                key.register()

    def _add_catalog_to_database(self, database_list: list["DeltaDatabase"]):
        warnings.warn(
            "This implementation part is deprecated in favour of class attributes."
        )
        for database in database_list:
            database.add_catalog(catalog=self)

    def name(self):
        """
        Returns the class name as snake_casing.

        This convention mirrors the deployment naming pattern.
        """
        return convert_pascal_casing_to_snake_casing(type(self).__name__)

    def register(self):
        """
        As Unity catalog creation is beyond the scope of this module, we merely set the
        """
        pyspark.sql.Catalog.setCurrentCatalog(catalogName=self.name)

    def is_registered(self):
        """
        Checks whether the catalog is registered.
        """
        raise NotImplementedError("Missing this feature currently.")


class DeltaDatabase(ABC):
    """
    Reflects the delta database / schema in Databricks / Spark.
    """

    def __init__(
        self,
        database_folder: str = "",
        comment="",
        models: Union[list["DeltaModel"], None] = None,
    ):
        if not database_folder:
            database_folder = self.name
        self._database_folder = database_folder
        self._comment = comment
        self._models = models
        self._catalog = None

    @property
    def catalog(self):
        """
        Returns if the
        Returns
        -------

        """
        return self._catalog

    def add_catalog(self, catalog: DeltaCatalog):
        """
        Attaches a (parent) catalog to the database.
        """
        self._catalog = catalog

    @property
    def name(self):
        """
        Returns the class name as snake casing.

        Note: Mirrors deployment naming, which uses snake_casing.
        """
        return convert_pascal_casing_to_snake_casing(type(self).__name__)

    @property
    def unity_catalog_object_name(self):
        """
        Returns the Unity Catalog valid name, which uses snake_casing.
        """
        names = [self._catalog.name, self.name]
        return UNITY_CATALOG_NAMING_SEPARATOR.join(names)

    def register(self):
        """
        1. Ensures that the database is attached to a registered catalog.
        2. Creates the database in the catalog if it does not exists.
        3. Creates all models registereds to this database.
        """
        if self.catalog is None:
            raise CatalogDoesNotExists(
                "Catalog for the Delta Database is None, but must be specified."
            )
        if self.catalog.name != pyspark.sql.Catalog.currentCatalog():
            raise CatalogException(
                f"Database attached catalog {self.catalog.name}"
                f" is not the current catalog."
            )
        if not spark.catalog.databaseExists(self.name):
            spark.sql(f"CREATE DATABASE {self.name}")
        return self

    def is_registered(self) -> bool:
        """
        1. Checks whether catalog of the database is registered
        2. Checks whether the database is registered to the catalog.
        """
        if self.catalog is None:
            raise CatalogDoesNotExists(
                "Catalog for the Delta Database is None, but must be specified."
            )
        if self.catalog.name != pyspark.sql.Catalog.currentCatalog():
            raise CatalogException(
                f"Database attached catalog {self.catalog.name}"
                f" is not the current catalog."
            )
        if not pyspark.sql.Catalog.databaseExists(self.name):
            raise DatabaseException(
                f"Database {self.name} does not"
                f" exists under its catalog {self.catalog.name}"
            )
        return True

    def register_models(self):
        """
        Registers all instance and class attributes of the type DeltaModel to the database.
        """
        spark.catalog.setCurrentDatabase(self.name)
        for key, value in self.__dict__.items():
            if key.startswith("_"):
                continue
            if value is not None and isinstance(key, DeltaModel):
                key.register()
        return self


class DeltaModel(ABC, DeltaModelStructure):
    """
    Interface class instantiation a delta table in spark.

    # the attribute set is intended to follow
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Catalog.createTable.html

    Usage pattern:
    """

    def __init__(self, folder_name: Union[str, None], description: str = "", **kwargs):
        self._folder_name = None
        if not folder_name:
            self._folder_name = self.name
        else:
            self._folder_name = folder_name
        self._description = description
        self._options = kwargs
        self.set_instance_attr_as_cls_attr()
        self._database = None

    def add_database(self, database: "DeltaDatabase"):
        """
        Sets the table database to be database.
        """
        self._database = database

    def unity_catalog_name(self) -> str:
        """
        Returns the full table name as a string, including catalog_name, schema_name and table_name

        Assume that the table is attached to a database, which is attached to a catalog.
        """
        names = [self.schema.catalog.name, self.schema.name, self.name]
        return UNITY_CATALOG_NAMING_SEPARATOR.join(names)

    @property
    def schema_name(self) -> str:
        """
        Returns the schema name as a string.
        """
        return self.schema.name

    @property
    def name(self):
        """
        Returns the class name, using snake casing.

        Note: Mirrors deployment naming, which uses snake_casing.
        """
        return convert_pascal_casing_to_snake_casing(type(self).__name__)

    def register_model(self):
        """
        Registers the DeltaModel to a DeltaDatabase.

        Procedure:
        1. Ensures that the model is attached to a registered schema.
        2. Creates the database in the catalog if it does not exists.
        """
        if self._database is None:
            raise DataModelException(
                f"No database is registered to the current DeltaModel {self.name}"
            )
        assert self.database.is_registered()
        if spark.catalog.tableExists(self.name) == False:
            spark.catalog.createTable(
                tableName=f"{self.name}",
                path=self.name,
                source="delta",
                schema=self.schema(),
                description=self._description,
                **self._options,
            )

    def is_registered(self):
        """
        1. Checks whether database of the table is registered.
        2. Checks whether the table is registered to the database.
        """
        self._database.is_registered()
        return
