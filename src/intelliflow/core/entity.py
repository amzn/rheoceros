# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from intelliflow.core.serialization import Serializable


class CoreData(Serializable):
    """Provide basic dunder implementations for core entities and mechanism to marshal them across different layers of
    application development (e.g from/to JSON)
    """

    def __eq__(self, other) -> bool:
        return type(other) is type(self) and self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        return hash(tuple(sorted(self.__dict__.items())))

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({','.join([f'{name}={repr(value)}' for name, value in self.__dict__.items()])})"

    def __str__(self) -> str:
        return self.__repr__()
