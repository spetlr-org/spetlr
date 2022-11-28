import difflib
from typing import Dict, Iterable, List

from pyspark.sql import DataFrame

from atc.etl.transformer import Transformer
from atc.exceptions import AtcException


class FuzzySelectException(AtcException):
    pass


class NoMatchException(FuzzySelectException):
    pass


class NonUniqueException(AtcException):
    pass


class FuzzySelectTransformer(Transformer):
    """To construct a FuzzySelectTransformer,
    give it a list of columns that you want to see in the output.

    The fuzzy aspect is that difflib is used to find the best
    approximate source column for each column name. The returned
    df will contain the columns as given to this transformer.

    If you would like to check the set of columns that your
    dataframe will be transformed into you can directly call
    the method find_best_mapping and inspect the returned mapping.
    """

    def __init__(self, columns: Iterable[str], match_cutoff=0.6):
        super().__init__()
        self.columns = list(columns)
        self.match_cutoff = match_cutoff

    def find_best_mapping(self, in_columns: Iterable[str]) -> Dict[str, str]:
        """returns a dict mapping source to target columns
        as a case insensitive fuzzy match."""
        in_columns = {col.lower(): col for col in in_columns}
        mapping = dict()
        for col in self.columns:
            if not len(in_columns):
                NoMatchException("Not enough source columns to match from.")

            matches: List[str] = difflib.get_close_matches(
                col.lower(), list(in_columns.keys()), n=2, cutoff=self.match_cutoff
            )

            if not len(matches):
                raise NoMatchException(f"No suitable source column found for {col}")

            if len(matches) > 1:
                candidates = [
                    (
                        in_columns[match],
                        difflib.SequenceMatcher(a=col.lower(), b=match).ratio(),
                    )
                    for match in matches
                ]
                raise NonUniqueException(
                    f"No unique source column match for {col}: candidates: {candidates}"
                )

            source_match = in_columns[matches[0]]
            mapping[source_match] = col
            del in_columns[matches[0]]

        return mapping

    def process(self, df: DataFrame) -> DataFrame:
        mapping = self.find_best_mapping(df.columns)

        new_df = df
        for key, value in mapping.items():
            new_df = new_df.withColumnRenamed(key, value)

        new_df = new_df.select(*self.columns)
        return new_df
