# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import datetime
from enum import Enum, unique
from typing import Any

NameType = str


@unique
class Type(str, Enum):
    """Represents the types we support for :class:`Dimension`"""

    STRING = "STRING"
    DATETIME = "DATETIME"
    LONG = "LONG"


class Timezone(Enum):
    """Enum to provide lang (or Python API) agnostic standardization of Timezone declaration
    for DATE and TIMESTAMP fields.

    Generally appended as a param/arg to a Dimension.
    High-level APIs are expected to leverage this critical piece of information for those types
    to avoid dangerous operations, incompatibilities (particularly in inter-datasets operations).

    TODO remove this ENUM. We will need more semantics (tz conversion, diff) and hence a library like "pytz".
    Do a type declaration instead. Users will be expeced to provide compatible timezone info during app development
    """

    UTC = 0
    GMT = 0
    PST = -8
    EST = -5
    # TODO once PoC apps are updated
    PST_DELETEME = 2
    UTC_DELETEME = 1

    @property
    def tzinfo(self) -> datetime.tzinfo:
        # convert Timezone to tzinfo
        # Option 1 (but if we are going to rely on pytz, then we can actually remove this Enum)
        # from pytz import timezone
        # return timezone(self._timezone.value)
        return datetime.timezone(datetime.timedelta(hours=self.value), name=self.name)


@unique
class DatetimeGranularity(str, Enum):
    MINUTE = "MINUTE"
    HOUR = "HOUR"
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    YEAR = "YEAR"

    @classmethod
    def has_value(cls, value: Any) -> bool:
        return value in set(item.value for item in DatetimeGranularity)


DEFAULT_DATETIME_GRANULARITY: DatetimeGranularity = DatetimeGranularity.DAY
