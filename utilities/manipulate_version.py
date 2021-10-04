"""
Replace whatever version is in atc/__init__.py
with <major>.<minor>.<git rev-list --count>
This facilitates continuous release.
"""
import importlib.resources as res
import re
from subprocess import check_output


def main():
    version = int(check_output(['git', 'rev-list', '--count', 'HEAD']).decode().strip())
    with res.path("atc","__init__.py") as p:
        conts = open(p).read()
        new_conts = re.sub(r'__version__\s+=\s+"(\d+)\.(\d+)\.\w+"',
                           rf'__version__ = "\1.\2.{version}"',
                           conts,
                           count=1)
        with open(p,"w") as f:
            f.write(new_conts)


if __name__ == "__main__":
    main()
