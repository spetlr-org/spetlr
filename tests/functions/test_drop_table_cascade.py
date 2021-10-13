import unittest

@unittest.skip("Current test pipeline does not support delta tables yet.")
# This file should test the function "drop_table_cascade" in atc/functions
class MergeDfIntoTargetTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass
