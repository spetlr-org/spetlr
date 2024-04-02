"""
Main entrypoint for creating database and catalogs using Spark API classes
"""

# pylint: skip-file
from spetlr.spark_catalog.orchestrator import SparkModelsOrchestrator


def main():
    orchestrator = SparkModelsOrchestrator()
    orchestrator.execute()
