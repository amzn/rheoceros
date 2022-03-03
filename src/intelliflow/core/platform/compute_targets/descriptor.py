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

    def create_output_attributes(self, platform: "HostPlatform", user_attrs: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Compute descriptor asks the related construct/driver for default or mandatory compute attrs
        (that would possibly be passed into nodes/routes).
        """
        return None

    @abstractmethod
    def create_slot(self, output_signal: Signal) -> Slot:
        ...

    @abstractmethod
    def activation_completed(self, platform: "HostPlatform") -> None:
        """System calls this to allow descriptor to know that system is up&running, all of the underlying resources
        are created. If the compute target needs to do further provisioning that relies on system resources, now it is
        time to set them up."""
        pass
