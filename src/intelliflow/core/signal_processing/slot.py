# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum, unique
from typing import Any, Dict, List, Optional

from intelliflow.core.entity import CoreData
from intelliflow.core.permission import Permission

from .definitions import compute_defs
from .signal import Signal


@unique
class SlotType(Enum):
    SYNC_INLINED = 1
    # ASYNC_INLINED=2
    # SYNC_REMOTE=3
    # ASYNC_REMOTE=4
    # SYNC_BATCH_COMPUTE=5
    ASYNC_BATCH_COMPUTE = 6

    def is_inlined_compute(self) -> bool:
        """Inlined here means anything that will be executed within Processor's runtime context,
        and in the future it might contain ASYNC_INLINED as well.
        """
        return self in [SlotType.SYNC_INLINED]

    def is_batch_compute(self) -> bool:
        return self in [SlotType.ASYNC_BATCH_COMPUTE]


@unique
class SlotCodeType(str, Enum):
    EMBEDDED_SCRIPT = "embedded_script"
    # ex: (Scala/Java) Class Path  / Python module path (module.foo.bar)
    MODULE_PATH = "path"


class SlotCodeMetadata(CoreData):
    def __init__(
        self,
        code_type: SlotCodeType,
        target_entity: Optional[str] = None,
        target_method: Optional[str] = None,
        # ex: s3 paths, etc
        external_library_paths: Optional[List[str]] = None,
    ) -> None:
        self.code_type = code_type
        self.target_entity = target_entity
        self.target_method = target_method
        # ex: s3 paths, etc
        self.external_library_paths = external_library_paths


class SlotCode(str):
    def __new__(cls, value, meta: Optional[SlotCodeMetadata] = None):
        obj = str.__new__(cls, value)
        obj._metadata = meta
        return obj

    @property
    def metadata(self) -> SlotCodeMetadata:
        return self._metadata

    def __eq__(self, other) -> bool:
        if isinstance(other, SlotCode):
            return super().__eq__(other) and self._metadata == other.metadata
        elif isinstance(other, str):
            return super().__eq__(other) if self._metadata is None else False
        else:
            raise ValueError(f"Cannot compare SlotCode to an object of type {type(other)!r}")


class Slot:
    ### A class to encapsulate the code that can be executed by our compute constructs###
    def __init__(
        self,
        type: SlotType,
        code: str,
        code_lang: Optional[compute_defs.Lang],
        code_abi: Optional[compute_defs.ABI],
        extra_params: Optional[Dict[str, Any]],
        output_signal_desc: Optional[Signal],
        compute_permissions: List[Permission] = None,
        retry_count: int = 0,
    ) -> None:
        self.type = type
        self.code = code
        self.code_lang = code_lang
        self.code_abi = code_abi
        self.extra_params = extra_params
        self.output_signal_desc = output_signal_desc
        self.compute_permissions = compute_permissions
        self.retry_count = retry_count

    def __eq__(self, other) -> bool:
        return (
            type(self) is type(other)
            and self.type == other.type
            and self.code == other.code
            and self.code_lang == other.code_lang
            and self.code_abi == other.code_abi
            and self.extra_params == other.extra_params
            and self.output_signal_desc == other.output_signal_desc
            and self.compute_permissions == other.compute_permissions
            and self.retry_count == other.retry_count
        )

    def __hash__(self) -> int:
        return hash(
            (
                self.type,
                self.code,
                self.code_lang,
                self.code_abi,
                self.extra_params,
                self.output_signal_desc,
                self.compute_permissions,
                self.retry_count,
            )
        )

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(type={self.type!r}, code={self.code!r}, code_lang={self.code_lang!r}, "
            f" code_abi={self.code_abi!r}, extra_params={self.extra_params!r}, "
            f" output_signal_desc={self.output_signal_desc!r}, "
            f" compute_permissions={self.compute_permissions}, retry_count={self.retry_count!r})"
        )

    def check_integrity(self, other: "Slot") -> bool:
        return self == other

    @property
    def code_metadata(self) -> SlotCodeMetadata:
        if isinstance(self.code, SlotCode):
            if self.code.metadata:
                return self.code.metadata
        return SlotCodeMetadata(SlotCodeType.EMBEDDED_SCRIPT, None, None)

    @property
    def permissions(self) -> Optional[List[Permission]]:
        return getattr(self, "compute_permissions", None)

    @property
    def max_retry_count(self) -> int:
        return getattr(self, "retry_count", 0)


__test__ = {name: value for name, value in locals().items() if name.startswith("test_")}

if __name__ == "__main__":
    import doctest

    doctest.testmod(verbose=False)
