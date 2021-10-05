"""
Replace whatever version is in atc/__init__.py
with a version that is larger than whatever is already published on test.pypi.org
This facilitates continuous release.
"""
import json
import re
from urllib.request import urlopen

from packaging.version import parse

init_file_path = "src/atc/__init__.py"


def main():
    # find out what version to use
    pypi_v = get_test_pypi_version()
    local_v = get_local_version()
    if local_v > pypi_v:
        version = local_v.base_version
    else:
        version = f"{pypi_v.major}.{pypi_v.minor}.{pypi_v.micro+1}"

    # substitute the version into the file
    conts = open(init_file_path).read()
    new_conts = re.sub(
        r'__version__\s+=\s+"\d+\.\d+\.\w+"',
        rf'__version__ = "{version}"',
        conts,
        count=1,
    )
    with open(init_file_path, "w") as f:
        f.write(new_conts)

def get_local_version():
    with open(init_file_path) as f:
        conts = f.read()
        m=re.search(r'__version__\s+=\s+"(\d+\.\d+\.\w+)"', conts)
        if not m:
            return None
        version_string = m.group(1)

        #clean up the version
        v= parse(version_string)
        v=parse(v.base_version)  # remove any suffices
        return v


def get_test_pypi_version():
    test_pypi = json.load(urlopen("https://test.pypi.org/pypi/atc-dataplatform/json"))
    test_pypi_version = parse(test_pypi["info"]["version"])
    return test_pypi_version

if __name__ == "__main__":
    main()
