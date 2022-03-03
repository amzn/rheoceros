# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""Module Doc:

    TODO refer design doc
"""
import copy
from abc import ABC, abstractmethod
from typing import List

from intelliflow.core.serialization import Serializable, dumps, loads

from .constructs import BatchCompute, Diagnostics, ProcessingUnit, ProcessorQueue, RoutingTable, Storage


class Platform(Serializable, ABC):
    """An abstraction of runtime architecture of an RheocerOS application.

    Highl-level component diagram for Platform:

     TODO link to design doc
    """

    @abstractmethod
    def __init__(self) -> None:
        self._context_id: str
        self._storage: Storage
        self._processor: ProcessingUnit
        self._processor_queue: ProcessorQueue
        self._batch_compute: BatchCompute
        self._routing_table: RoutingTable
        self._diagnostics: Diagnostics

    def _serializable_copy_init(self, org_instance: "Platform") -> None:
        self._storage = org_instance._storage.serializable_copy()
        self._processor = org_instance._processor.serializable_copy()
        self._processor_queue = org_instance._processor_queue.serializable_copy()
        self._batch_compute = org_instance._batch_compute.serializable_copy()
        self._routing_table = org_instance._routing_table.serializable_copy()
        self._diagnostics = org_instance._diagnostics.serializable_copy()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(context_id={self._context_id}, {self.get_arch()})"

    def get_arch(self) -> str:
        return (
            repr(self._storage)
            + repr(self._processor)
            + repr(self._processor_queue)
            + repr(self._batch_compute)
            + repr(self._routing_table)
            + repr(self._diagnostics)
        )

    @property
    def context_id(self) -> str:
        return self._context_id

    @context_id.setter
    def context_id(self, value: str) -> None:
        self._context_id = value

    @property
    def storage(self) -> Storage:
        return self._storage

    @property
    def processor(self) -> ProcessingUnit:
        return self._processor

    @property
    def processor_queue(self) -> ProcessorQueue:
        return self._processor_queue

    @property
    def batch_compute(self) -> BatchCompute:
        return self._batch_compute

    @property
    def routing_table(self) -> RoutingTable:
        return self._routing_table

    @property
    def diagnostics(self) -> Diagnostics:
        # backwards compatibility
        # TODO remove before release
        return getattr(self, "_diagnostics", None)
