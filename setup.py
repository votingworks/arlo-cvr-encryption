#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

from glob import glob
from os.path import basename
from os.path import splitext

from setuptools import find_packages
from setuptools import setup

NAME = "arlo-e2e"
VERSION = "0.1.0"
LICENSE = "GNU Affero General Public License v3"
DESCRIPTION = "Arlo-e2e: Support for e2e verified risk-limiting audits."
AUTHOR = "VotingWorks"
AUTHOR_EMAIL = "dwallach@voting.works"
URL = "https://github.com/votingworks/arlo-e2e"
PROJECT_URLS = {
    "Changelog": "https://github.com/votingworks/arlo-e2e/blob/master/CHANGELOG.rst",
    "Issue Tracker": "https://github.com/votingworks/arlo-e2e/issues",
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
        "numpy==1.18.2",
        "pandas==1.0.4",
        "hypothesis==5.29.4",
        "typish==1.7.0",
        "jsons==1.1.2",
        "tqdm==4.47.0",
        "cryptography==2.9.2",
        "ray==0.8.7",
    ],
    # electionguard is also a requirement, but we're assuming that's being installed elsewhere, since
    # we're hanging on a dev branch, etc.
)
