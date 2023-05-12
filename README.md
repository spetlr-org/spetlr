# spetlr
A python SPark ETL libRary (SPETLR) for Databricks. 

Visit SPETLR official webpage: [https://spetlr.com/](https://spetlr.com/)

## Important Notes

This package can not be run or tested without access to pyspark.
However, installing pyspark as part of our installer gave issues when
other versions of pyspark were needed. Hence we took out the dependency
from our installer.

## Installation

Get it from PyPi 
[![PyPI version](https://badge.fury.io/py/spetlr.svg)](https://pypi.org/project/spetlr/)
[![PyPI](https://img.shields.io/pypi/dm/spetlr)](https://pypi.org/project/spetlr/)
```    
pip install spetlr
```

## Development Notes

To prepare for development please install these additional requirements:
 - Java 8
 - `pip install -r test_requirements.txt`

Then install the package locally

    python setup.py develop


### Testing

After installing the dev-requirements, execute tests by running

    pytest tests

If these tests don't pass, PRs will not be accepted. If you add features,
please include tests for these features.


### General Project Info
[![Github top language](https://img.shields.io/github/languages/top/spetlr-org/spetlr)](https://github.com/spetlr-org/spetlr)
[![Github stars](https://img.shields.io/github/stars/spetlr-org/spetlr)](https://github.com/spetlr-org/spetlr)
[![Github forks](https://img.shields.io/github/forks/spetlr-org/spetlr)](https://github.com/spetlr-org/spetlr)
[![Github size](https://img.shields.io/github/repo-size/spetlr-org/spetlr)](https://github.com/spetlr-org/spetlr)
[![Issues Open](https://img.shields.io/github/issues/spetlr-org/spetlr.svg?logo=github)](https://github.com/spetlr-org/spetlr/issues)

### Packages

### Build Status
