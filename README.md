# spetlr

A python ETL libRary (SPETLR) for Databricks powered by Apache SPark.

Visit SPETLR official webpage: [https://spetlr.com/](https://spetlr.com/)

# NEWS

Start Supporting DBR LTS14.3: [Follow the PR.](https://github.com/spetlr-org/spetlr/pull/177).
- Cluster test submission with spetlr-tools
- Upgrade to Python 3.10
- The spetlr library (probably except SQL connection with ODBC) still supports older LTS versions between 9.1 and 13.3, but only 14.3 is tested.
- SQL ODBC driver version 18 is suppoetd (this is a breaking change if you haven't upgraded your ODBC driver).
- Neweset CosmosDB connector is tested for compatibility with DBR LTS14.3.

# Table of Contents

- [Description](#description)
- [Important Notes](#important-notes)
- [Installation](#installation)
- [Development Notes](#development-notes)
- [Testing](#testing)
- [General Project Info](#general-project-info)
- [Contributing](#contributing)
- [Build Status](#build-status)
- [Releases](#releases)
- [Requirements](#requirements-and-dependencies)
- [Contact](#contact)

# Description

SPETLR has a lot of great tools for working with ETL in Databricks. But to make it easy for you to consider why you need
SPETLR here is a list of the core features:

* ETL framework: A common ETL framework that enables reusable transformations in an object-oriented manner. Standardized
  structures facilitate cooperation in large teams.

* Integration testing: A framework for creating test databases and tables before deploying to production in order to
  ensure reliable and stable data platforms. An additional layer of data abstraction allows full integration testing.

* Handlers: Standard connectors with commonly used options reduce boilerplate.

For more information, visit SPETLR official webpage: [https://spetlr.com/](https://spetlr.com/)

# Important Notes

This package can not be run or tested without access to `pyspark`.
However, installing `pyspark` as part of our installer gave issues when
other versions of `pyspark` were needed. Hence we took out the dependency
from our installer.

# Installation

Install SPETLR from PyPI:
[![PyPI version](https://badge.fury.io/py/spetlr.svg)](https://pypi.org/project/spetlr/)
[![PyPI](https://img.shields.io/pypi/dm/spetlr)](https://pypi.org/project/spetlr/)

```    
pip install spetlr
```

# Development Notes

To prepare for development, please install these additional requirements:

- Java 8
- `pip install -r test_requirements.txt`

Then install the package locally

    python setup.py develop

## Testing

### Local tests

After installing the dev-requirements, execute tests by running:

    pytest tests

These tests are located in the `./tests/local` folder and only require a Python interpreter. Pull requests will not be
accepted if these tests do not pass. If you add new features, please include corresponding tests.

### Cluster tests

Tests in the `./tests/cluster` folder are designed to run on a Databricks cluster.
The [Pre-integration Test](https://github.com/spetlr-org/spetlr/blob/main/.github/workflows/pre-integration.yml)
utilizes Azure Resource deployment - and can only be run by the spetlr-org admins.

To deploy the necessary Azure resources to your own Azure Tenant, run the following command:

```powershell
.\.github\deploy\deploy.ps1 -uniqueRunId "yourUniqueId"
```
Be aware that the applied name for *uniqueRunId* should only contain lower case and numbers, and its length should not
exceed 12 characters.

Afterward, execute the following commands:

```powershell 
.\.github\submit\build.ps1
.\.github\submit\submit_test_job.ps1
```

# General Project Info

[![Github top language](https://img.shields.io/github/languages/top/spetlr-org/spetlr)](https://github.com/spetlr-org/spetlr)
[![Github stars](https://img.shields.io/github/stars/spetlr-org/spetlr)](https://github.com/spetlr-org/spetlr)
[![Github forks](https://img.shields.io/github/forks/spetlr-org/spetlr)](https://github.com/spetlr-org/spetlr)
[![Github size](https://img.shields.io/github/repo-size/spetlr-org/spetlr)](https://github.com/spetlr-org/spetlr)
[![Issues Open](https://img.shields.io/github/issues/spetlr-org/spetlr.svg?logo=github)](https://github.com/spetlr-org/spetlr/issues)
[![PyPI spetlr badge](https://img.shields.io/pypi/v/spetlr)](https://pypi.org/project/spetlr/)

# Contributing

Feel free to contribute to SPETLR. Any contributions are appreciated - not only new features, but also if you find a way
to improve SPETLR.

If you have a suggestion that can enhance SPETLR, please fork the repository and create a pull request. Alternatively,
you can open an issue with the "enhancement" tag.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/NewSPETLRFeature`)
3. Commit your Changes (`git commit -m 'Add some SEPTLRFeature'`)
4. Push to the Branch (`git push origin feature/NewSPETLRFeature`)
5. Open a Pull Request

# Build Status

[![Post-Integration](https://github.com/spetlr-org/spetlr/actions/workflows/post-integration.yml/badge.svg)](https://github.com/spetlr-org/spetlr/actions/workflows/post-integration.yml)

# Releases

Releases to PyPI is an Github Action which needs to be manually triggered.

[![Release](https://github.com/spetlr-org/spetlr/actions/workflows/release.yml/badge.svg)](https://github.com/spetlr-org/spetlr/actions/workflows/release.yml)
[![PyPI spetlr badge](https://img.shields.io/pypi/v/spetlr)](https://pypi.org/project/spetlr/)


# Requirements and dependencies
The library has three txt-files at the root of the repo. These files defines three levels of requirements:
- `requirements_install.txt` - this file contains the required libraries to be able to install spetlr.
- `requirements_test.txt` - libraries required to run unit- and integration tests
- `requirements_dev.txt` - libraries required in the development process in order to contribute to the repo

All libraries and their dependencies are added with a fixed version to the configuration file `setup.cfg` using the defined requirements from `requirements_install.txt`.

To __upgrade__ the the dependencies in the `setup.cfg` file do the following:

1. Create a new branch
2. Run `upgrade_requirements.ps1` in your terminal
3. Commit the changes the script has made to the cfg file. If there are no changes, everything is up to date.
4. The PR runs all tests and ensure that the library is compliant with any updates

Note that if it is desired to upgrade a dependency, but not to its newest version, it is possible to set the desired version in the `requirements_install.txt`, then this will be respected by the upgrade script.

# Contact

For any inquiries, please use the [SPETLR Discord Server](https://discord.gg/p9bzqGybVW).
