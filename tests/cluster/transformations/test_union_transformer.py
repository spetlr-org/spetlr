from spetlrtools.testing import DataframeTestCase, TestHandle

from spetlr.etl import Orchestrator
from spetlr.etl.extractors import SimpleExtractor
from spetlr.etl.loaders import SimpleLoader
from spetlr.etl.transformers import UnionTransformer
from spetlr.spark import Spark


class MergeDfIntoTargetTest(DataframeTestCase):
    def test_01_union(self):
        schema = "i integer, s string"

        th1 = TestHandle(provides=Spark.get().createDataFrame([(1, "a")], schema))
        th2 = TestHandle(provides=Spark.get().createDataFrame([(1, "a")], schema))

        th3 = TestHandle()

        o = Orchestrator()
        o.extract_from(SimpleExtractor(th1, "th1"))
        o.extract_from(SimpleExtractor(th2, "th2"))
        o.transform_with(UnionTransformer())
        o.load_into(SimpleLoader(th3))
        o.execute()

        self.assertEqualSchema(th1.provides.schema, th3.overwritten.schema)
