# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import codecs
import copy
import datetime
import json
import zlib
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Generic, Optional, Type, TypeVar
from uuid import uuid4

import dill as pickle
from dill.source import getsource

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

# JSON Serialization Definitions
_CORE_DATA_ID_TYPE = int
_SERIALIZED_CORE_DATA_ID_TYPE = str
_SERIALIZED_CORE_DATA_TYPE_FIELD_NAME = "_intelliflow_core_data_type"
_SERIALIZED_CORE_DATA_ID_FIELD_NAME = "_intelliflow_core_data_id"
_SERIALIZED_CORE_DATA_ID_REF_FIELD_NAME = "_intelliflow_core_data_ref"
_SERIALIZED_CORE_DATA_FIELD_NAME = "_intelliflow_core_data"
_SERIALIZATION_CACHE = Dict[_CORE_DATA_ID_TYPE, _SERIALIZED_CORE_DATA_ID_TYPE]
_DESERIALIZATION_CACHE = Dict[_SERIALIZED_CORE_DATA_ID_TYPE, "JSONSerializable"]

_SERIALIZED_PICKLED_FIELD_NAME = "_intelliflow_pickled_data"

_SERIALIZED_DICT_KEY = "_intelliflow_dict_key"


class JSONSerializable(ABC, Generic[_Serialized]):
    @abstractmethod
    def serializable_copy(self) -> _Serialized:
        ...

    def _serializable_full_class_name(self) -> str:
        klass = self.__class__
        module = klass.__module__
        if module == "builtins":  # e.g 'str' -> 'builtins.str'
            return klass.__qualname__
        return module + "." + klass.__qualname__

    @classmethod
    def _serialize_dict_key(cls, key: Any) -> Any:
        if key is None or isinstance(key, (str, int, float, bool)):
            return key
        return f"{_SERIALIZED_DICT_KEY}_{dumps(key, compress=True)}"

    @classmethod
    def _deserialize_dict_key(cls, key: Any) -> Any:
        if isinstance(key, str) and key.startswith(_SERIALIZED_DICT_KEY):
            return loads(key[len(_SERIALIZED_DICT_KEY) + 1 :])
        return key

    @classmethod
    def _serialize_value(cls, value: Any, cache: _SERIALIZATION_CACHE) -> Any:
        if value is None:
            return value
        if isinstance(value, JSONSerializable):
            serialized_value = value._to_json(cache)
        else:
            pickle = False
            if isinstance(value, set):
                serialized_value = [cls._serialize_value(v, cache) for v in value]
                _type = set
            elif isinstance(value, frozenset):
                serialized_value = [cls._serialize_value(v, cache) for v in value]
                _type = frozenset
            elif isinstance(value, list):
                serialized_value = [cls._serialize_value(v, cache) for v in value]
                _type = list
            elif isinstance(value, tuple):
                serialized_value = tuple(cls._serialize_value(v, cache) for v in value)
                _type = tuple
            elif isinstance(value, dict):
                serialized_value = {cls._serialize_dict_key(k): cls._serialize_value(v, cache) for k, v in value.items()}
                _type = dict
            elif isinstance(value, Callable):
                serialized_value = getsource(value)
                _type = callable
                pickle = True
            else:
                serialized_value = repr(value)
                _type = type(value)
                pickle = True

            serialized_value = {
                _SERIALIZED_CORE_DATA_TYPE_FIELD_NAME: _type.__name__,
                _SERIALIZED_CORE_DATA_FIELD_NAME: serialized_value,
                _SERIALIZED_PICKLED_FIELD_NAME: dumps(value, compress=True) if pickle else None,
            }
        return serialized_value

    def _to_json_serializable(self, cache: _SERIALIZATION_CACHE) -> Dict[str, Any]:
        new_dict = dict()
        for key, value in self.__dict__.items():
            new_dict[key] = self._serialize_value(value, cache)
        return new_dict

    def _to_json(self, cache: _SERIALIZATION_CACHE) -> Dict[str, Any]:
        object_id: _SERIALIZED_CORE_DATA_ID_TYPE = id(self)
        if object_id in cache:
            # e.g {"_intelliflow_core_data_ref": "1A11-2B22-C333-D444"}
            return {_SERIALIZED_CORE_DATA_ID_REF_FIELD_NAME: cache[object_id]}

        serialized_entity_id: _SERIALIZED_CORE_DATA_ID_TYPE = str(uuid4())
        cache[object_id] = serialized_entity_id
        serialized_data = {
            _SERIALIZED_CORE_DATA_ID_FIELD_NAME: serialized_entity_id,
            _SERIALIZED_CORE_DATA_TYPE_FIELD_NAME: self._serializable_full_class_name(),
            _SERIALIZED_CORE_DATA_FIELD_NAME: self.serializable_copy()._to_json_serializable(cache),
        }

        return serialized_data

    def to_json(self) -> str:
        cache: _SERIALIZATION_CACHE = dict()

        serializable_dict = self._to_json(cache)

        return json.dumps(serializable_dict)

    @classmethod
    def from_json(cls, json_str: str) -> "JSONSerializable":
        json_serializable: Dict[str, Any] = json.loads(json_str)
        deserialization_entity_cache = dict()
        # 1- first pass to establish entity hierarchy and fill cache with entity references
        root: JSONSerializable = cls._from_json(json_serializable, deserialization_entity_cache)
        # 2- use the newly populated cache to establish references within the entity hierarchy
        cls._set_references(root, deserialization_entity_cache)
        return root

    @classmethod
    def _from_json(cls, json_serializable: Dict[str, Any], deserialization_entity_cache: _DESERIALIZATION_CACHE) -> "JSONSerializable":
        root_full_cls_name = json_serializable[_SERIALIZED_CORE_DATA_TYPE_FIELD_NAME]
        root_type = cls._deserialize_full_class(root_full_cls_name)
        root: JSONSerializable = root_type.__new__(root_type)
        # 1- first pass to establish entity hierarchy and fill cache with entity references
        root._from_json_serializable(json_serializable, deserialization_entity_cache)
        return root

    def _from_json_serializable(self, json_serializable: Dict[str, Any], cache: _DESERIALIZATION_CACHE) -> None:
        """Set object attribures using the dict data and add an entry to cache for this object's reference"""
        serialized_entity_id = json_serializable[_SERIALIZED_CORE_DATA_ID_FIELD_NAME]
        attrs: Dict[str, Any] = json_serializable[_SERIALIZED_CORE_DATA_FIELD_NAME]
        for key, value in attrs.items():
            self.__dict__[key] = self._json_deserialize_value(value, cache)
        cache[serialized_entity_id] = self

    @classmethod
    def _deserialize_full_class(cls, full_class_name: str) -> Type:
        components = full_class_name.split(".")
        mod = __import__(components[0])
        for comp in components[1:]:
            mod = getattr(mod, comp)
        return mod

    def _json_deserialize_value(self, value: Any, cache: _DESERIALIZATION_CACHE) -> Any:
        if value is None:
            return value

        if isinstance(value, dict):
            if _SERIALIZED_CORE_DATA_ID_REF_FIELD_NAME in value:
                # return if it is a reference (they will be bound later)
                return value

            # don't need to do anything else if pickled data is still there
            if value.get(_SERIALIZED_PICKLED_FIELD_NAME, None):
                return loads(value[_SERIALIZED_PICKLED_FIELD_NAME])

            if _SERIALIZED_CORE_DATA_ID_FIELD_NAME in value:
                return self._from_json(value, cache)
            else:
                if _SERIALIZED_CORE_DATA_TYPE_FIELD_NAME not in value:
                    pass
                _type_str = value[_SERIALIZED_CORE_DATA_TYPE_FIELD_NAME]
                serialized_data = value[_SERIALIZED_CORE_DATA_FIELD_NAME]
                if _type_str == set.__name__:
                    return set(self._json_deserialize_value(v, cache) for v in serialized_data)
                elif _type_str == frozenset.__name__:
                    return set(self._json_deserialize_value(v, cache) for v in serialized_data)
                elif _type_str == list.__name__:
                    return [self._json_deserialize_value(v, cache) for v in serialized_data]
                elif _type_str == tuple.__name__:
                    return tuple(self._json_deserialize_value(v, cache) for v in serialized_data)
                elif _type_str == dict.__name__:
                    return {self._deserialize_dict_key(k): self._json_deserialize_value(v, cache) for k, v in serialized_data.items()}
                elif _type_str == callable.__name__:
                    return self._deserialize_callable(serialized_data)
                else:
                    return eval(serialized_data)

        raise ValueError(f"Value (type={type(value).__name__}) cannot be deserialized! Bad serialized data: {value!r}")

    @classmethod
    def _deserialize_callable(cls, callable: str) -> Callable:
        _locals = dict()
        exec(callable, globals(), _locals)
        return next(iter(_locals.items()))[1]

    @classmethod
    def _set_references(cls, obj: Any, cache: _DESERIALIZATION_CACHE) -> Any:
        if isinstance(obj, list):
            return [cls._set_references(o, cache) for o in obj]
        elif isinstance(obj, tuple):
            return tuple(cls._set_references(o, cache) for o in obj)
        elif isinstance(obj, (set, frozenset)):
            return {cls._set_references(o, cache) for o in obj}
        elif isinstance(obj, dict):
            if _SERIALIZED_CORE_DATA_ID_REF_FIELD_NAME in obj:
                return cache[obj[_SERIALIZED_CORE_DATA_ID_REF_FIELD_NAME]]
            else:
                return {k: cls._set_references(v, cache) for k, v in obj.items()}
        elif isinstance(obj, JSONSerializable):
            references: Dict[str, Any] = dict()

            for key, value in obj.__dict__.items():
                references[key] = cls._set_references(value, cache)

            obj.__dict__.update(references)
        return obj


class Serializable(JSONSerializable[_Serialized]):
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
