import unittest

import atc
import io
from contextlib import redirect_stdout



class TestSimple(unittest.TestCase):

    def test_hello(self):
        with io.StringIO() as buf, redirect_stdout(buf):
            atc.hello()
            output = buf.getvalue()
            self.assertEqual("Hello from ATC.Net\n", buf.getvalue())


if __name__ == '__main__':
    unittest.main()
