#!/bin/bash

# Accept python version arguments for consistency with other bootstrap scripts
# Arguments are required - script will fail if not provided
PYTHON_MAJOR=$1
PYTHON_MINOR=$2
PYTHON_BUILD=$3

# Use /usr/local/bin path consistently with python-version.sh and python-version_7xx.sh
IF_PYTHON=/usr/local/bin/python${PYTHON_MAJOR}.${PYTHON_MINOR}

echo "Installing IntelliFlow core dependencies using Python ${PYTHON_MAJOR}.${PYTHON_MINOR}.${PYTHON_BUILD}"
echo "Python path: ${IF_PYTHON}"

# Verify Python installation exists
if [ ! -f "${IF_PYTHON}" ]; then
    echo "ERROR: Python ${PYTHON_MAJOR}.${PYTHON_MINOR} not found at ${IF_PYTHON}"
    echo "This script should run after python-version.sh or python-version_7xx.sh"
    exit 1
fi

# Dependencies - using the specific Python version installed by previous bootstrap script
sudo ${IF_PYTHON} -m pip install boto3 s3fs

echo "IntelliFlow core dependencies installation completed"