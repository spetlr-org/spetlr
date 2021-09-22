import setuptools

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='atc-dataplatform',
    author='ATC.Net',
    author_email='atcnet.org@gmail.com',
    description='A common set of python libraries for DataBricks',
    keywords='databricks, pyspark',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/atc-net/atc-dataplatform',
    project_urls={
        'Documentation': 'https://github.com/atc-net/atc-dataplatform',
        'Bug Reports':
        'https://github.com/atc-net/atc-dataplatform/issues',
        'Source Code': 'https://github.com/atc-net/atc-dataplatform',
    },
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where='src'),
    classifiers=[
        # see https://pypi.org/classifiers/
        'Development Status :: 2 - Pre-Alpha',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3 :: Only',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    install_requires=[
        'pyspark',
        'pyyaml',
        'sqlparse',
        'more_itertools'
    ],
    extras_require={
        'dev': ['check-manifest'],
        # 'test': ['coverage'],
    },
    # entry_points={
    #     'console_scripts': [  # This can provide executable scripts
    #         'run=atc:main',
    # You can execute `run` in bash to run `main()` in src/atc/__init__.py
    #     ],
    # },
)
