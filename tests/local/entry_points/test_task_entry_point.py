import unittest

from spetlr.entry_points.task_entry_point import TaskEntryPoint


class TestModuleHelper(unittest.TestCase):
    def test_task_as_abstract_method(self):
        self.assertTrue(hasattr(TaskEntryPoint.task, "__isabstractmethod__"))


if __name__ == "__main__":
    unittest.main()
