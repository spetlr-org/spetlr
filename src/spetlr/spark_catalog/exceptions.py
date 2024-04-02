"""
Custom exceptions for the PySpark Catalog API module.
"""


class CatalogException(Exception):
    """
    Custom exception intended to be raised for Catalog-related errors
    """


class DatabaseException(Exception):
    """
    Custom exception intended to be raised for database/schema-related errors
    """


class DataModelException(Exception):
    """
    Custom exception intended to be raised data model-related errors
    """


class CatalogDoesNotExists(CatalogException):
    """
    Raise when a Unity Catalog does not exists, when it is supposed to.
    """
