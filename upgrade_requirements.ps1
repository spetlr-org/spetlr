"""
A utility script for updating library versions in a Python project's 
requirements file or `setup.cfg`.

Find the spetlr-freeze-req in spetlr-tools.

This script:
- Parses command-line arguments to specify the input requirements file
- Creates a temporary virtual environment to isolate library installations.
- Reads the input requirements file, installs the libraries within the virtual environment, 
    and fetches their latest versions using `pip list --format json`.
- Filters out specified libraries to exclude from updates.
- Updates the `install_requires` section in `setup.cfg`
"""

spetlr-freeze-req requirements_install.txt --cfg
