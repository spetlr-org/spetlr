# from typing import Type
# from unittest import TestCase
# from unittest.mock import Mock
#
# from spetlrtools.testing import DataframeTestCase, TestHandle
#
# from spetlr.etl import Orchestrator, Transformer
# from spetlr.etl.extractors import SimpleExtractor
# from spetlr.etl.loaders import SimpleLoader
# from spetlr.spark import Spark
# from spetlr.transformers import CountryToAlphaCodeTransformer
#
#
# def ETLTransformerTester(trans) -> Orchestrator:
#     empty_df = Spark.get().createDataFrame(data=[], schema="col1 string")
#
#     dh_extract_1 = TestHandle(provides=empty_df)
#     dh_extract_2 = TestHandle(provides=empty_df)
#
#     oc = Orchestrator()
#
#     oc.extract_from(SimpleExtractor(handle=dh_extract_1, dataset_key="df_1"))
#
#     oc.extract_from(SimpleExtractor(handle=dh_extract_2, dataset_key="df_2"))
#
#     oc.transform_with(
#         trans,
#     )
#
#     return oc
#
#
# class test_this(DataframeTestCase):
#     def test_01(self):
#         trans = CountryToAlphaCodeTransformer
#         ETLTransformerTests(self, trans)
#
#
# def ETLTransformerTests(ut: TestCase, trans):
#     empty_df = Spark.get().createDataFrame(data=[], schema="col1 string")
#
#     trans.process = Mock(return_value=empty_df)
#
#     trans_A = trans(
#         col_name="countryCol",
#         dataset_input_keys=["df_1"],
#         dataset_output_key="df_trans",
#         consume_inputs=False,
#     )
#     dh_load = TestHandle()
#     oc_test_A = ETLTransformerTester(trans_A)
#     oc_test_A.load_into(
#         SimpleLoader(handle=dh_load, mode="overwrite",
#         dataset_input_keys=["df_trans"])
#     )
#
#     oc_test_A.execute()
#
#     trans_B = trans(
#         col_name="countryCol",
#         dataset_input_keys=["df_1"],
#         dataset_output_key="df_trans",
#         consume_inputs=True,
#     )
#
#     oc_test_B = ETLTransformerTester(
#         trans_B,
#     )
#     oc_test_B.load_into(
#         SimpleLoader(handle=dh_load, mode="overwrite", dataset_input_keys=["df_1"])
#     )
#
#     with ut.assertRaises(KeyError) as cm:
#         oc_test_B.execute()
#
#     # Since it is consuming, it should not be able to find df_1
#     ut.assertEqual(cm.exception.args[0], "df_1")
