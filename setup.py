#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

from glob import glob
from os.path import basename
from os.path import splitext

from setuptools import find_packages
from setuptools import setup

NAME = "arlo-cvr-encryption"
VERSION = "0.1.0"
LICENSE = "GNU Affero General Public License v3"
DESCRIPTION = "Arlo-cvr-encryption: Support for verified risk-limiting audits."
AUTHOR = "VotingWorks"
AUTHOR_EMAIL = "dwallach@gmail.com"
URL = "https://github.com/votingworks/arlo-cvr-encryption"
PROJECT_URLS = {
    "Changelog": "https://github.com/votingworks/arlo-cvr-encryption/blob/master/CHANGELOG.rst",
    "Issue Tracker": "https://github.com/votingworks/arlo-cvr-encryption/issues",
}
CLASSIFIERS = [
    # http://pypi.python.org/pypi?%3Aaction=list_classifiers
    "Development Status :: 3 - Alpha",  # TODO Update when Stable
    "Intended Audience :: Developers",
    "License :: OSI Approved :: GNU Affero General Public License v3",
    "Operating System :: Unix",
    "Operating System :: POSIX",
    "Operating System :: MacOS",
    "Operating System :: Microsoft :: Windows",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: Implementation :: CPython",
    "Topic :: Utilities",
]

setup(
    name=NAME,
    version=VERSION,
    license=LICENSE,
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    packages=find_packages("src"),
    package_dir={"": "src"},
    py_modules=[splitext(basename(path))[0] for path in glob("src/*/*.py")],
    include_package_data=True,
    zip_safe=False,
    classifiers=CLASSIFIERS,
    project_urls=PROJECT_URLS,
    python_requires=">=3.8",
    install_requires=[
        "gmpy2==2.1.0b5",
        "numpy==1.21.2",
        "pandas==1.3.3",
        "hypothesis==6.23.0",
        "typish==1.9.3",
        "jsons==1.5.1",
        "tqdm==4.62.3",
        "cryptography==3.4.8",
        "flask==2.0.1",
        "ray[default]==1.6.0",
        "pillow==8.3.2",
        "qrcode==7.3",
        "more-itertools==8.10.0",
        "boto3==1.18.48",
        "boto3-stubs[s3,ec2]==1.18.48",
    ],
    # ElectionGuard is also a requirement, but we're assuming that's being installed elsewhere, since
    # we're using a forked version of it.
    # Note: this file needs to be kept in sync with Pipfile.
)
