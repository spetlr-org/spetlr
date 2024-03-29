import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from spetlr.etl.log import LogTransformer


class CountLogTransformer(LogTransformer):
    """
    A subclass of LogTransformer that logs the number of rows in a DataFrame.

    Arguments:
        log_name (str): The name for the log entry.
        dataset_input_keys (List[str], optional): A list of input dataset keys
            that this transformer will process. Defaults to None.
        dataset_output_key (str, optional): The key for the output dataset
            generated by this transformer. If not provided, a random UUID will
            be used as the default value. It is not required for when using the
            LogOrchestrator.
        consume_inputs (bool, optional): Flag to control dataset consuming behavior.
            Defaults to True
    """

    def log(self, df: DataFrame) -> DataFrame:
        return df.select(F.count("*").alias("Count"))
