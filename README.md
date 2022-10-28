# atc-dataplatform
A common set of python libraries for DataBricks. 

## Important Notes

This package can not be run or tested without access to pyspark.
However, installing pyspark as part of our installer gave issues when
other versions of pyspark were needed. Hence we took out the dependency
from our installer.

## Installation

Get it from PyPi 
[![PyPI version](https://badge.fury.io/py/atc-dataplatform.svg)](https://pypi.org/project/atc-dataplatform/)
[![PyPI](https://img.shields.io/pypi/dm/atc-dataplatform)](https://pypi.org/project/atc-dataplatform/)
```    
pip install atc-dataplatform
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
[![Github top language](https://img.shields.io/github/languages/top/atc-net/atc-dataplatform)](https://github.com/atc-net/atc-dataplatform)
[![Github stars](https://img.shields.io/github/stars/atc-net/atc-dataplatform)](https://github.com/atc-net/atc-dataplatform)
[![Github forks](https://img.shields.io/github/forks/atc-net/atc-dataplatform)](https://github.com/atc-net/atc-dataplatform)
[![Github size](https://img.shields.io/github/repo-size/atc-net/atc-dataplatform)](https://github.com/atc-net/atc-dataplatform)
[![Issues Open](https://img.shields.io/github/issues/atc-net/atc-dataplatform.svg?logo=github)](https://github.com/atc-net/atc-dataplatform/issues)

### Packages

### Build Status
