"""
Replace whatever version is in src/VERSION.txt
with a version that is larger than whatever is already published on test.pypi.org
This facilitates continuous release.
"""

import json
from urllib.request import urlopen

from packaging.version import parse

version_file_path = "src/VERSION.txt"


def main():
    # find out what version to use
    pypi_v = max(get_version("test.pypi.org"), get_version("pypi.org"))
    local_v = get_local_version()
    if local_v > pypi_v:
        version = local_v.base_version
    else:
        version = f"{pypi_v.major}.{pypi_v.minor}.{pypi_v.micro+1}"

    with open(version_file_path, "w") as f:
        f.write(version)


def get_local_version():
    with open(version_file_path) as f:
        # clean up the version
        v = parse(f.read())
        v = parse(v.base_version)  # remove any suffices
        return v


def get_version(host):
    try:
        test_pypi = json.load(urlopen(f"https://{host}/pypi/spetlr/json"))
        test_pypi_version = parse(test_pypi["info"]["version"])
        return test_pypi_version
    except:  # noqa: E722
        return parse("0.0.0")


if __name__ == "__main__":
    main()
