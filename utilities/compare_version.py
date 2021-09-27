"""
This script will compare the version as set in the currently installed 'atc' package,
to the versions of TestPyPi and PyPi repositories, using their rest APIs.
Only if the current version is higher, will this script return with zero (error free)
return code.
"""
import json
import sys
from urllib.request import urlopen

from packaging.version import parse

import atc


def main():
    pypi = json.load(urlopen("https://pypi.org/pypi/atc-dataplatform/json"))
    pypi_version = parse(pypi["info"]["version"])
    test_pypi = json.load(urlopen("https://test.pypi.org/pypi/atc-dataplatform/json"))
    test_pypi_version = parse(test_pypi["info"]["version"])

    version = parse(atc.__version__)

    if not (version > test_pypi_version):
        print(
            f"Current version {atc.__version__}"
            f" is not ahead of TestPyPi's {test_pypi['info']['version']}"
        )
        return 1
    if not (version > pypi_version):
        print(
            f"Current version {atc.__version__}"
            f" is not ahead of PyPi's {pypi['info']['version']}"
        )
        return 1

    print(f"Version is newer than published versions.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
