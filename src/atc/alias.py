"""
Pyspark local sessions on windows insist on finding the python interpreter
as python3 on the PATH.
Regardless of settings such as spark.pyspark.python which have no apparent effect.
This fake script is added to the path as "python3" in you virtual environment
and calls the same interpreter as the "python" executable.
This makes spark testing work on windows using local sessions.
"""
import os
import sys


def python3():
    os.execv(sys.executable, sys.argv)
