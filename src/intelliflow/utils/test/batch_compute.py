# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Deployable module (within src) to provide common definitions for tests.

Test modules are not deployed, so some code-paths are not easy or impossible to test by using the definitions from
within test or test_integration modules.
"""


def limit(df, limit):
    return df.limit(limit)
