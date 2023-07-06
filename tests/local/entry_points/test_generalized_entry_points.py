import unittest

from spetlr.entry_points.generalized_task_entry_point import prepare_keyword_arguments


class TestArgumnetHandler(unittest.TestCase):
    def test_good_parameters(self):
        def f(a, b=1, c=1):
            pass

        kwargs = {"a": 4, "b": 5, "c": 6}
        result = prepare_keyword_arguments(f, kwargs)
        self.assertEqual(result, kwargs)

    def test_too_many_parameters(self):
        def f(a, b=1, c=1):
            pass

        kwargs = {"b": 5, "c": 6, "d": 4}
        result = prepare_keyword_arguments(f, kwargs)
        self.assertEqual(set(result.keys()), set("bc"))
