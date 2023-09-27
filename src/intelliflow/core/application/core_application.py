# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from enum import Enum, unique

from intelliflow.core.entity import CoreData
from intelliflow.core.platform.platform import Platform
from intelliflow.core.serialization import DeserializationError, SerializationError, dumps, loads

logger = logging.getLogger(__name__)


@unique
class ApplicationState(Enum):
    INACTIVE = 1
    PROVISIONING = 2
    ACTIVE = 3
    PAUSED = 4
    TERMINATING = 5
    DELETED = 6


ApplicationID = str


class _PersistedState(CoreData):
    def __init__(self, state: ApplicationState, serialized_active_context: str) -> None:
        self.state = state
        self.serialized_active_context = serialized_active_context


class CoreApplication(ABC):
    def __init__(self, id: ApplicationID, platform: Platform, enforce_runtime_compatibility: bool = True) -> None:
        assert id, "Application ID cannot be empty or None!"
        # TODO make platform (underlying conf) check ApplicationID
        self._id = id
        self._enforce_runtime_compatibility = enforce_runtime_compatibility
        self._incompatible_runtime = False
        self._platform: Platform = platform
        self._platform._enforce_runtime_compatibility = enforce_runtime_compatibility
        # platform sync/initialization should happen when context ID is set
        self._platform.context_id = id
        # now platform is bound to this context, we can use it to check existing state
        self._sync()

    def _sync(self) -> None:
        synced = False
        try:
            if self._platform.storage.check_object([CoreApplication.__name__], _PersistedState.__name__):
                persisted_state: _PersistedState = self._load()
                self._state: ApplicationState = persisted_state.state
                self._active_context: "Context" = loads(persisted_state.serialized_active_context)
                self._active_context._deserialized_init(self._platform)
                synced = True
        except (ModuleNotFoundError, AttributeError, SerializationError, DeserializationError) as error:
            self._incompatible_runtime = True
            logger.critical(f"Active application state is not compatible! Error: {error!r}")
            if self._enforce_runtime_compatibility:
                raise error

        if not synced:
            self._state: ApplicationState = ApplicationState.INACTIVE
            self._active_context: "Context" = None

    @property
    def id(self) -> ApplicationID:
        return self._id

    @property
    def platform(self) -> Platform:
        return self._platform

    @property
    def state(self) -> ApplicationState:
        return self._state

    @property
    def active_context(self) -> "Context":
        return self._active_context

    def is_active(self) -> bool:
        return True if self._active_context else False

    def _save(self) -> None:
        core_state = _PersistedState(self._state, self._active_context.serialize(True))
        persisted_state: str = dumps(core_state)
        self._platform.storage.save(persisted_state, [CoreApplication.__name__], _PersistedState.__name__)

    def _load(self) -> _PersistedState:
        persisted_state: str = self._platform.storage.load([CoreApplication.__name__], _PersistedState.__name__)
        return loads(persisted_state)

    @abstractmethod
    def refresh(self) -> None:
        pass
