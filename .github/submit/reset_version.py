"""
Replace whatever version is in atc/__init__.py
with a version that is constant
This facilitates integration testing.
"""
import re
import sys

init_file_path = sys.argv[-1]


def main():
    # substitute the version into the file
    conts = open(init_file_path).read()
    new_conts = re.sub(
        r'__version__\s+=\s+"\d+\.\d+\.\w+"',
        rf'__version__ = "1.0.0"',
        conts,
        count=1,
    )
    with open(init_file_path, "w") as f:
        f.write(new_conts)


if __name__ == "__main__":
    main()
