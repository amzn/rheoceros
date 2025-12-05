#!/bin/bash

# e.g install Python3.8
sudo amazon-linux-extras enable python$1.$2
sudo yum install python$1$2 -y

# set pyspark environment variables
export PYSPARK_PYTHON=python$1.$2
export PYSPARK_DRIVER_PYTHON=python$1.$2

sudo ln -sf /usr/bin/python$1.$2 /usr/local/bin/python$1
sudo ln -sf /usr/bin/python$1.$2 /usr/local/bin/python$1.$2
