# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import List

from intelliflow.core.entity import CoreData


class PermissionContext(str, Enum):
    DEVTIME = "DEVTIME"
    RUNTIME = "RUNTIME"
    ALL = "ALL"


class Permission(CoreData):
    """Universal representation of a permission in RheocerOS along with the specification of context (dev/runtime) that
    it is going to be attached to."""

    def __init__(self, resource: List[str], action: List[str], context: PermissionContext = PermissionContext.ALL) -> None:
        self.resource = resource
        self.action = action
        self.context = context

    def __eq__(self, other: "Permission") -> bool:
        return set(self.resource) == set(other.resource) and set(self.action) == set(other.action) and self.context == other.context

    def __hash__(self) -> int:
        return hash((frozenset(self.resource), frozenset(self.action)))
