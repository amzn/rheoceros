# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import codecs
import copy
import zlib
from abc import ABC, abstractmethod
from typing import Any, Generic, Optional, TypeVar

# import pickle
import dill as pickle

# FUTURE remove if we ever rely on 'cloudpickle' since it auto-registers this
# why do we need this? In platforms where cloudpickle is used (as in notebooks where pyspark uses it), then this
# registration is there. But when we need to deserialize "KeyError: 'ClassType' is raised.
if "ClassType" not in pickle._dill._reverse_typemap:
    pickle._dill._reverse_typemap["ClassType"] = type


"""Module to contain our Serialization primitives and mappings.

Requirements:
- our serializer should not call __init__ on new objects during deserialization.

"""

"""
## DEBUG
# Ex: debug how PySpark hacks namedtuple and it causes a dependency on pyspark.serializers._restore on other envs
# that do not/should not contain 'pyspark'. This happens on PySpark enabled notebooks:
#   https://spark.apache.org/docs/2.4.1/api/python/_modules/pyspark/serializers.html

import io
from dill import Unpickler

class Dummy:
    def __init__(self, *args, **kwargs):
        pass

    def __call__(self, *args, **kwargs):
        return self

    def __getattr__(self, *args, **kwargs):
        return self

    def __setattr__(self, *args, **kwargs):
        return self


class IFUnpickler(Unpickler):
    def find_class(self, module, name):
        if module.startswith('pyspark'):
            return Dummy
        return super().find_class(module, name)
"""

INTELLIFLOW_COMPRESSION_MAGIC: str = "_IntelliFlow_ZIP_"


def loads(dump_str: str) -> Any:
    # Enable for DEBUG on de-serialization errors
    # return IFUnpickler(io.BytesIO(codecs.decode(dump_str.encode(), 'base64'))).load()
    if dump_str.startswith(INTELLIFLOW_COMPRESSION_MAGIC):
        dump_str = dump_str[len(INTELLIFLOW_COMPRESSION_MAGIC) :]
        decoded_dump = zlib.decompress(codecs.decode(dump_str.encode(), "base64"))
    else:
        decoded_dump: str = codecs.decode(dump_str.encode(), "base64")
    return pickle.loads(decoded_dump)


def dumps(obj: Any, compress: Optional[bool] = False) -> str:
    pickled: bytes = pickle.dumps(obj)
    if compress:
        pickled = codecs.encode(zlib.compress(pickled), "base64").decode()
        return INTELLIFLOW_COMPRESSION_MAGIC + pickled
    return codecs.encode(pickled, "base64").decode()


_Serialized = TypeVar("_Serialized")


class Serializable(ABC, Generic[_Serialized]):
    def serialize(self, check_unsafe=False) -> str:
        serializable: _Serialized = self
        if check_unsafe:
            serializable = self.serializable_copy()
        return dumps(serializable)

    @classmethod
    def deserialize(cls, serialized_str: str) -> _Serialized:  # type: ignore
        return loads(serialized_str)

    def serializable_copy(self) -> _Serialized:
        shallow_copy = copy.copy(self)
        shallow_copy._serializable_copy_init(self)
        return shallow_copy

    def _serializable_copy_init(self, org_instance: _Serialized) -> None:
        """This is a chance for classes to align their unsafe, nonserializable fields
        Default behaviour is nothing, which means that the ultimate result would
        be equivalent to unsafe=True.

        So nonserializable fields, overrides of this method is expected to call serialize(True)
        on other complex serializable sub-entities or just remove them.
        """
        pass
