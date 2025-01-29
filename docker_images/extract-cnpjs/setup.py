#!/usr/bin/env python
from setuptools import setup

setup(
    name="extract-cnpjs",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["extract_cnpjs"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
        "flake8",
        "parameterized",
        "singer-tools",
        "boto3"
    ],
    entry_points="""
    [console_scripts]
    extract-cnpjs=extract_cnpjs:main
    """,
    packages=["extract_cnpjs"],
    package_data={"schemas": ["extract_cnpjs/schemas/*.json"]},
    include_package_data=True,
)
