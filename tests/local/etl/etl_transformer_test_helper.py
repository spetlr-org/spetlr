from spetlrtools.testing import TestHandle

from spetlr.etl import Orchestrator
from spetlr.etl.extractors import SimpleExtractor
from spetlr.spark import Spark


def ETLTransformerTester(trans) -> Orchestrator:
    """
    This method is a helper for the test_transformer.py

    A transformer is applied in a OETL setup.

    """
    empty_df = Spark.get().createDataFrame(data=[], schema="col1 string")

    dh_extract_1 = TestHandle(provides=empty_df)
    dh_extract_2 = TestHandle(provides=empty_df)

    oc = Orchestrator()

    oc.extract_from(SimpleExtractor(handle=dh_extract_1, dataset_key="df_1"))

    oc.extract_from(SimpleExtractor(handle=dh_extract_2, dataset_key="df_2"))

    oc.transform_with(
        trans,
    )

    return oc
