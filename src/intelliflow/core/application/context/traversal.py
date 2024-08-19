# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod


class ContextVisitor(ABC):
    @abstractmethod
    def visit(self, node: "Node") -> None: ...
