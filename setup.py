import setuptools
import re

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()


def get_version():
    with open("src/atc/__init__.py") as f:
        return re.search(r'__version__\s+=\s+"(\d+\.\d+\.\w+)"', f.read()).group(1)


setuptools.setup(
    name="atc-dataplatform",
    author="ATC.Net",
    version=get_version(),
    author_email="atcnet.org@gmail.com",
    description="A common set of python libraries for DataBricks",
    keywords="databricks, pyspark",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/atc-net/atc-dataplatform",
    license_files=["LICENSE"],
    project_urls={
        "Documentation": "https://github.com/atc-net/atc-dataplatform",
        "Bug Reports": "https://github.com/atc-net/atc-dataplatform/issues",
        "Source Code": "https://github.com/atc-net/atc-dataplatform",
    },
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    classifiers=[
        # see https://pypi.org/classifiers/
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3 :: Only",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=["pyyaml", "sqlparse", "deprecation"],
    extras_require={
        "dev": ["check-manifest"],
        # 'test': ['coverage'],
    },
    entry_points={
        "console_scripts": [
            # see explanation in file:
            "python3=atc.alias:python3",
            "atc-dataplatform-git-hooks=atc.formatting.git_hooks:main",
        ],
    },
)
