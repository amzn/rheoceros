Tests for rheoceros
=====================

This directory contains unit-tests for rheoceros:

Prerequisites
-------------

Check the requirements files inside this folder.

The easiest way to install these (and some useful pytest add-ons) is running
```
pip install -U -r requirements-test.txt
```

Running the Tests
-----------------

To run the tests, navigate to the root directory of the PyInstaller project and
run the following command:

    py.test

Or, to speed up test runs by sending tests to multiple CPUs:

    py.test -n NUM

Or, targetting sub-modules:

    py.test test/intelliflow/core/application
    py.test test/intelliflow/core/platform
    py.test test/intelliflow/core/platform -k "not tests/intelliflow/core/platform/test_commons.py"

Run all tests matching `test_aws_application`:

    py.test -k test_aws_application