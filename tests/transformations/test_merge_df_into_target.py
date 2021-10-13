import unittest

@unittest.skip("Current test pipeline does not support delta tables yet.")
# This file should test the transformation "merge_df_into_target" in atc/transformations
class MergeDfIntoTargetTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

