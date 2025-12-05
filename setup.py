# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
from setuptools import find_packages, setup
from src.intelliflow import __version__ as version

cmdclass_value = {}
options_value = {}

#CLI_SCRIPTS = [
#    'my_app= intelliflow.tools:foo_main',
#]

# use 'requirements.txt' as baseline here + virtualenv for bundling
REQUIRED_PACKAGES = [
    'boto3 >= 1.41.1',
    'python-dateutil >= 2.9.0',
    'dill >= 0.4.0',
    'packaging >= 25.0',
    'requests >= 2.32.4',
    'responses >= 0.23.3',
    'shortuuid >= 1.0.13',
    'overrides >= 3.1.0',
    'validators >= 0.11.0',
    'virtualenv'
]

TEST_PACKAGES = [
    'moto',
    'pytest',
    'mock'
]

data_files = []
# Declare your non-python data files:
# Files underneath configuration/ will be copied into the build preserving the
# subdirectory structure if they exist.
dirs_to_copy = [
    "configuration",
    "src"
]

for dir_path in dirs_to_copy:
    for root, dirs, files in os.walk(dir_path):
        data_files.append(
            (os.path.relpath(root, dir_path), [os.path.join(root, f) for f in files])
        )

setup(
    name="rheoceros",
    python_requires=">=3.10",
    version=version,
    description="rheoceros is a cloud-based data science / AI / ML workflow development framework.",
    keywords="aws cloud data model ai workflow ai-team ml mesh collaboration event flow automation functional low-code"
             "machine learning feature engineering emr glue athena sagemaker serverless",
    author="Amazon.com Inc.",
    author_email="dexcovery@amazon.com",
    license="Apache 2.0",
    download_url='https://github.com/amzn/rheoceros/releases',

    packages=find_packages(where="src", exclude=("test", "test_integration")),
    package_dir={"": "src"},
    #entry_points={
    #    'cli_scripts': CLI_SCRIPTS,
    #},
    install_requires=REQUIRED_PACKAGES,
    tests_require=TEST_PACKAGES,
    test_suite='test',

    # include data files
    data_files=data_files,
    include_package_data=True,
    root_script_source_version="default-only",
    test_command="pytest",
    # Use custom sphinx command which adds an index.html that's compatible with
    # code.amazon.com links.
    doc_command="amazon_doc_utils_build_sphinx",

    options=options_value,
    cmdclass=cmdclass_value,

    # Enable build-time format checking
    check_format=False,

    # Enable type checking
    test_mypy=False,

    # Enable linting at build time
    test_flake8=False,

)
