"""
This script will compare the version as set in the currently installed 'atc' package,
to the versions of TestPyPi and PyPi repositories, using their APIs.
Only if the current version is different, will this script return with zero (error free)
return code.
"""
import re
import sys
from urllib.request import urlopen

from packaging.version import parse

import atc


def main():
    pypi = get_versions("https://pypi.org/simple/", "atc-dataplatform")
    pypi_test = get_versions("https://test.pypi.org/simple/", "atc-dataplatform")

    version = parse(atc.__version__)

    if version in pypi_test:
        print(f"Current version {atc.__version__} already exists in TestPyPi")
        return 1
    if version in pypi:
        print(f"Current version {atc.__version__} already exists in PyPi")
        return 1

    print(f"Version is different from published versions.")
    return 0


def get_versions(repo: str, package: str):
    """The interface https://pypi.org/simple/<package>/ is the one that pip
    uses to get versions. The problem with the interface
    "https://pypi.org/pypi/atc-dataplatform/json" is that it does not show
    pre-releases."""
    document = urlopen(repo + package + "/").read().decode()
    vers = []
    for match in re.findall(package + r"-(\d+\.\d+\.\d+\w+)\.tar\.gz", document):
        vers.append(parse(match))
    vers.sort()
    return vers


if __name__ == "__main__":
    sys.exit(main())
