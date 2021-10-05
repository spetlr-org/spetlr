"""
Replace whatever version is in atc/__init__.py
with <major>.<minor>.<git rev-list --count>
This facilitates continuous release.
"""
import re
from subprocess import check_output

init_file_path = "src/atc/__init__.py"


def main():
    version = int(check_output(["git", "rev-list", "--merges", "--count", "HEAD"]).decode().strip())
    conts = open(init_file_path).read()
    new_conts = re.sub(
        r'__version__\s+=\s+"(\d+)\.(\d+)\.\w+"',
        rf'__version__ = "\1.\2.{version}"',
        conts,
        count=1,
    )
    with open(init_file_path, "w") as f:
        f.write(new_conts)


if __name__ == "__main__":
    main()
