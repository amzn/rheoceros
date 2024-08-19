# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from typing import Any, Callable, ClassVar, Dict, List, Optional

from intelliflow.core.permission import Permission
from intelliflow.core.serialization import dumps
from intelliflow.core.signal_processing import Signal, Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.slot import SlotType


class ComputeDescriptor(ABC):
    def parametrize(self, platform: "HostPlatform") -> None:
        """System calls this to allow descriptor impl to align parameters on the new Slot object to be returned
        from within create_slot."""
        pass

    def create_output_attributes(
        self, platform: "HostPlatform", inputs: List[Signal], user_attrs: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Compute descriptor asks the related construct/driver for default or mandatory compute attrs
        (that would possibly be passed into nodes/routes).
        """
        return None

    @abstractmethod
    def create_slot(self, output_signal: Signal) -> Slot: ...

    @abstractmethod
    def activation_completed(self, platform: "HostPlatform") -> None:
        """System calls this to allow descriptor to know that system is up&running, all of the underlying resources
        are created. If the compute target needs to do further provisioning that relies on system resources, now it is
        time to set them up."""
        pass

    def describe_slot(self) -> Dict[str, Any]:
        return {"code": self.create_slot(None).describe_code()}


class DelegatedComputeDescriptor(ComputeDescriptor):
    def __init__(self, delegation: Optional[ComputeDescriptor] = None):
        super().__init__()
        self._delegation = delegation

    @property
    def delegation(self):
        return self._delegation

    def parametrize(self, platform: "HostPlatform") -> None:
        if self.delegation:
            self.delegation.parametrize(platform)

    def create_output_attributes(
        self, platform: "HostPlatform", inputs: List[Signal], user_attrs: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        if self.delegation:
            return self.delegation.create_output_attributes(platform, inputs, user_attrs)
        return None

    def create_slot(self, output_signal: Signal) -> Slot:
        permissions = []
        if self.delegation:
            notification_slot = self.delegation.create_slot(output_signal)
            permissions = notification_slot.permissions
        return Slot(SlotType.SYNC_INLINED, dumps(self), None, None, dict(), None, permissions, 0)

    def activation_completed(self, platform: "HostPlatform") -> None:
        if self.delegation:
            self.delegation.activation_completed(platform)
