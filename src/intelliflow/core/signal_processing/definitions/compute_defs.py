# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum, unique


@unique
class Lang(int, Enum):
    PYTHON = 1
    SCALA = 2
    SPARK_SQL = 3
    PRESTO_SQL = 4
    HIVE = 5
    PIG = 6


@unique
class ABI(int, Enum):
    GLUE_EMBEDDED = 1
    _AMZN_RESERVED_1 = 2
    PARAMETRIZED_QUERY = 3


__test__ = {name: value for name, value in locals().items() if name.startswith("test_")}
if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=False)
