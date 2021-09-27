# atc-dataplatform
A common set of python libraries for DataBricks

## Important Notes

This package can not be run or tested without access to pyspark.
However, installing pyspark as part of our installer gave issues when
other versions of pyspark were needed. Hence we took out the dependency
from our installer.

## Development Notes

### Git Hooks

Please install the git hooks in your repository by running

    python utilities/git_hooks.py

The hooks will help you catch problems locally instead of having to wait for the PR pipeline to fail.

### Testing

To run the tests you will need these dependencies
 - Java 8
 - `pip install -r test_requirements.txt`

Then, execute tests by running

    python setup.py develop
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
