# Copyright (c) 2020 Raytheon BBN Technologies Corp.
# See LICENSE.txt for details.

"""
Setuptools setup module for packaging the tc-bbn-py project.
"""

from setuptools import setup, find_packages
import sys

# Set the avro install_requires string according to the target python version
# and the target Avro version.
AVRO_VER = "1.8.2-tupty"
#AVRO_PYTHON3 = "avro-python3==" + AVRO_VER
#AVRO_PYTHON2 = "avro==" + AVRO_VER
AVRO_PYTHON3 = "avro-python3==" + AVRO_VER
AVRO_PYTHON2 = "avro==" + AVRO_VER

AVRO_PYTHON = AVRO_PYTHON3 if sys.version_info > (3, 0) else AVRO_PYTHON2
TEST_MODULE = "tc.tests"

setup(
    name="tc-bbn-py",
    version="20.20190211.0",
    description="Python bindings for TC infrastructure services",
    test_suite=TEST_MODULE,
    packages=find_packages(exclude=[TEST_MODULE]),
    install_requires=[AVRO_PYTHON, "avro_json_serializer==0.5", "confluent-kafka==0.11.4", "quickavro==0.1.22", "simplejson", "six"],
    dependency_links=[
        "https://github.com/gth828r/avro/tarball/record-validation-fix#egg=avro-" + AVRO_VER + "&subdirectory=lang/py",
        "https://github.com/gth828r/avro-python3/tarball/record-validation-fix#egg=avro-python3-" + AVRO_VER + "&subdirectory=lang/py3",
    ],
    author="Raytheon BBN Technologies",
    author_email="tc-ta3@bbn.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4"
        # FIXME: add the license here too"
        # "",
    ],
    # FIXME: set this
    # license=""
    # FIXME: set URL
    # url=""
)
