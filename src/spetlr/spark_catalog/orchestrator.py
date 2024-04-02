"""
Main orchestrator class for creating databases and tables from Spark classes.
"""

from spetlr.spark_catalog.models import DataScienceCatalog


# pylint: skip-file


class SparkModelsOrchestrator:
    """
    Orchestrator for creating databases and tables from spark classes.

    Note: Builder pattern
    """

    def __init__(self):

        return

    def execute(self):
        """
        Executes the orchestrator.
        """
        DataScienceCatalog().register()
        DataScienceCatalog().register_all_databases()
        DataScienceCatalog().register_all_databases_and_models()

        # Other usage patterns
        # Ability to access minor sub-databases and tables.
        DataScienceCatalog.ClaimsClassModel.register_models()
        DataScienceCatalog.ClaimsClassModel.Alarm.register()
