Tests for rheoceros
=====================

This directory contains unit-tests for rheoceros:

Prerequisites
-------------

Check the requirements files inside this folder.

Running the Tests
-----------------

Option 1 - IDE (PyCharm)
--------------

You can run tests individually via the GUI and PyCharm.
You must set up PyCharm to use PyTest, and then you can select and right click a specific test to run it.
Make sure you have PyCharm setup to use Pytest:

To configure Pytest, In PyCharm,
navigate to

    → File → Settings →Tools → Python Integrated Tools

And under "testing", select "pytest" as the default test runner.

Option 2
--------

Navigate to the root directory of the project and run the following command:

    py.test

Or, to speed up test and run them in parallel:

    py.test -n NUM

Or, targeting sub-modules:

    py.test test/intelliflow/core/application
    py.test test/intelliflow/core/platform
    py.test test/intelliflow/core/platform -k "not tests/intelliflow/core/platform/test_commons.py"

Run all tests matching `test_aws_application`:

    py.test -k test_aws_application