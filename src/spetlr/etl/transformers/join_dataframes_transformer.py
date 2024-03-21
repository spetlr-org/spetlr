from typing import List

from pyspark.sql import DataFrame

from spetlr.etl import Transformer
from spetlr.etl.types import dataset_group
from spetlr.exceptions import (
    ColumnDoesNotExistException,
    MoreThanTwoDataFramesException,
)


class JoinDataframesTransformer(Transformer):
    """
    This transformer joins two DataFrames together.

    Attributes:
    ----------
        first_dataframe_join_key : str
            the name of the column that will be joined on in the first DataFrame
        second_dataframe_join_key : str
            the name of the column that will be joined on in the second DataFrame
        join_type : str
            the type of the join
        dataset_input_keys : Union[str, List[str]]
            list of dataset keys
        dataset_output_key : str
            identifier for the output DataFrame

    Methods
    -------
    process_many(dataset: dataset_group):
        returns the two input DataFrames joined together
    """

    def __init__(
        self,
        first_dataframe_join_key: str,
        second_dataframe_join_key: str,
        join_type: str = "inner",
        dataset_input_keys: List[str] = None,
        dataset_output_key: str = None,
        consume_inputs: bool = True,
    ):
        if len(dataset_input_keys) > 2:
            raise MoreThanTwoDataFramesException(
                """More than two DataFrames are specified in 'dataset_input_keys'.
                This transformer can only join two DataFrames at a time."""
            )

        super().__init__(
            dataset_input_keys=dataset_input_keys,
            dataset_output_key=dataset_output_key,
            consume_inputs=consume_inputs,
        )
        self.first_dataframe_join_key = first_dataframe_join_key
        self.second_dataframe_join_key = second_dataframe_join_key
        self.join_type = join_type

    def process_many(self, dataset: dataset_group) -> DataFrame:
        """
        Takes a dictionary of datasets and joins two specified dataframes together.
        """

        first_df = dataset[self.dataset_input_keys[0]]
        second_df = dataset[self.dataset_input_keys[1]]

        if self.first_dataframe_join_key not in first_df.columns:
            raise ColumnDoesNotExistException(
                f"""'{self.first_dataframe_join_key}' cannot be found in
                the first DataFrame."""
            )

        if self.second_dataframe_join_key not in second_df.columns:
            raise ColumnDoesNotExistException(
                f"""'{self.second_dataframe_join_key}' cannot be found in
                the second DataFrame."""
            )

        return first_df.join(
            second_df,
            first_df[self.first_dataframe_join_key]
            == second_df[self.second_dataframe_join_key],
            how=self.join_type,
        )
