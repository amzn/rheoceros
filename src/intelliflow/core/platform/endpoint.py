# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Optional


class DevEndpointType(str, Enum):
    LOCAL_NOTEBOOK = "LOCAL_NOTEBOOK"
    SAGEMAKER_NOTEBOOK_INSTANCE = "SAGEMAKER_NOTEBOOK_INSTANCE"


class DevEndpoint(ABC):
    def __init__(self, type: DevEndpointType, name: str, attrs: Dict[str, Any]) -> None:
        self._type = type
        self._name = name
        self._attrs: Dict[str, Any] = attrs
        self._uri: str = None

    @property
    def type(self) -> DevEndpointType:
        return self._type

    @property
    def name(self) -> str:
        return self._name

    @property
    def attrs(self) -> Dict[str, Any]:
        return self._attrs

    @property
    def uri(self) -> str:
        return self._uri

    def _set_uri(self, uri: str) -> None:
        self._uri = uri

    @abstractmethod
    def start(self, wait_till_active=False) -> None:
        pass

    @abstractmethod
    def is_active(self) -> bool:
        pass

    @abstractmethod
    def stop(self, wait_till_inactive=False) -> None:
        pass

    @abstractmethod
    def generate_url(self, session_duration_in_secs: Optional[int] = None) -> str:
        pass
