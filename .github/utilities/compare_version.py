import json
import sys
from urllib.request import urlopen

from packaging.version import parse

import atc

pypi = json.load(urlopen("https://pypi.org/pypi/atc-dataplatform/json"))


if parse(atc.__version__) > parse(pypi["info"]["version"]):
    print(
        f"Current version {atc.__version__}"
        f" is ahead of PyPi's {pypi['info']['version']} ✅"
    )
    sys.exit(0)
else:
    print(
        f"Current version {atc.__version__}"
        f" is not ahead of PyPi's {pypi['info']['version']} ❌"
    )
    sys.exit(1)
