# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum, unique
from typing import Any, ClassVar, Dict, List, Optional, Set, Union

from intelliflow.core.entity import CoreData
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.dimension_constructs import AnyVariant, DimensionSpec


@unique
class MetricDimension(str, Enum):
    """Represents the dimensions of a metric signal
    refer 'dimension_spec' in
         TODO alarming and metrics doc
    """

    NAME = "name"
    STATISTIC = "statistic"
    PERIOD = "period"
    TIME = "time"


METRIC_NAME_DIMENSION_TYPE = Type.STRING
METRIC_STATISTIC_DIMENSION_TYPE = Type.STRING
METRIC_PERIOD_DIMENSION_TYPE = Type.LONG
METRIC_TIME_DIMENSION_TYPE = Type.DATETIME


MetricSubDimensionMapType = Dict[str, str]
MetricSubDimensionMap = dict
MetricStatType = Dict[str, Union[str, Dict[str, Union[str, dict]]]]


METRIC_DIMENSION_SPEC: DimensionSpec = DimensionSpec.load_from_pretty(
    {
        MetricDimension.NAME: {
            type: METRIC_NAME_DIMENSION_TYPE,
            MetricDimension.STATISTIC: {
                type: METRIC_STATISTIC_DIMENSION_TYPE,
                MetricDimension.PERIOD: {
                    type: METRIC_PERIOD_DIMENSION_TYPE,
                    MetricDimension.TIME: {type: METRIC_TIME_DIMENSION_TYPE, "format": "%Y-%m-%d %H:%M:%S"},
                },
            },
        }
    }
)


@unique
class MetricStatistic(str, Enum):
    SAMPLE_COUNT = "SampleCount"
    AVERAGE = "Average"
    SUM = "Sum"
    MINIMUM = "Minimum"
    MAXIMUM = "Maximum"

    @classmethod
    def PERCENTILE(cls, integer_part: int, fractional_part_digit_1: Optional[int] = 0, fractional_part_digit_2: Optional[int] = 0) -> str:
        """Impl of
        https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Percentiles
        """
        if integer_part < 0 or integer_part > 100:
            raise ValueError(f"Integer part should be between 0 and 100 for {cls.__name__}::percentile!")
        if fractional_part_digit_1 < 0 or fractional_part_digit_1 > 9:
            raise ValueError(f"First digit of fractional part should be between 0 and 9 for {cls.__name__}::percentile!")
        if fractional_part_digit_2 < 0 or fractional_part_digit_2 > 9:
            raise ValueError(f"Second digit of fractional part should be between 0 and 9 for {cls.__name__}::percentile!")

        return f"p{integer_part}.{fractional_part_digit_1}{fractional_part_digit_2}"

    @classmethod
    def from_raw(cls, value: Any) -> Union["MetricStatistic", str]:
        if isinstance(value, MetricStatistic):
            return value

        value_str = str(value)
        try:
            return MetricStatistic(value_str)
        except ValueError:
            pass

        percentile_parts = value_str.split(".")
        int_part = percentile_parts[0][1:]  # remove leading 'p'
        frac_part = percentile_parts[1]
        return MetricStatistic.PERCENTILE(int(int_part), int(frac_part[0]), int(frac_part[1]))

    def __str__(self) -> str:
        return self.value


@unique
class MetricPeriod(int, Enum):
    TEN_SECS = 10
    THIRTY_SECS = 30
    ONE_MIN = 60

    @classmethod
    def SECONDS(cls, seconds: int) -> int:
        if (
            seconds < cls.TEN_SECS
            or (seconds > cls.TEN_SECS and seconds < cls.THIRTY_SECS)
            or (seconds > cls.THIRTY_SECS and seconds < cls.ONE_MIN)
            or (seconds % cls.ONE_MIN) != 0
        ):
            raise ValueError(
                f"MetricPeriod::SECONDS should be either 10 seconds, 30 seconds or a multiple of 60. " f"Value {seconds} is not valid!"
            )
        return seconds

    @classmethod
    def MINUTES(cls, minutes: int) -> int:
        if minutes < 1:
            raise ValueError(f"MetricPeriod::MINUTES should a positive integer. Value {minutes} is not valid!")
        return minutes * cls.ONE_MIN

    @classmethod
    def from_raw(cls, value: Any) -> Union["MetricPeriod", int]:
        int_value = int(value)
        try:
            return MetricPeriod(int_value)
        except ValueError:
            pass

        return MetricPeriod.SECONDS(int_value)

    def __str__(self) -> str:
        return str(self.value)


class MetricDimensionValues(CoreData):
    def __init__(
        self, name: str, statistic: Union[MetricStatistic, str], period: Union[MetricPeriod, int], time: Union[datetime, str]
    ) -> None:
        self.name = name
        self.statistic = statistic
        self.period = period
        self.time = time

    @classmethod
    def from_raw_values(cls, dim_values: List[Any]) -> "MetricDimensionValues":
        return MetricDimensionValues(
            dim_values[0],
            MetricStatistic.from_raw(dim_values[1])
            if dim_values[1] != AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR
            else AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR,
            MetricPeriod.from_raw(dim_values[2])
            if dim_values[2] != AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR
            else AnyVariant.ANY_DIMENSION_VALUE_SPECIAL_CHAR,
            dim_values[3],
        )


class MetricExpression(str):
    DEFAULT_ALIAS: ClassVar[str] = "if_target_metric_expression"

    def __new__(cls, expression: str, alias: Optional[str] = None):
        obj = str.__new__(cls, expression)
        obj._alias = alias
        return obj

    @property
    def alias(self) -> str:
        return self._alias


# Metric Emission related defs
class MetricStatisticData(CoreData):
    def __init__(self, SampleCount: float, Sum: float, Minimum: float, Maximum: float) -> None:
        self.SampleCount = SampleCount
        self.Sum = Sum
        self.Minimum = Minimum
        self.Maximum = Maximum


class MetricValueCountPairData(CoreData):
    def __init__(
        self,
        Value: float,
        # adapt AWS CW default behaviour
        Count: Optional[float] = 1,
    ) -> None:
        self.Value = Value
        # adapt AWS CW default behaviour
        self.Count = Count


@unique
class AlarmDimension(str, Enum):
    """Represents the dimensions of an alarm signal
    refer 'dimension_spec' in
         TODO doc link
    """

    STATE_TRANSITION = "state_transition"
    TIME = "time"


ALARM_STATE_TRANSITION_DIMENSION_TYPE = Type.STRING
ALARM_TIME_DIMENSION_TYPE = Type.DATETIME


@unique
class AlarmState(str, Enum):
    OK = "OK"
    ALARM = "ALARM"
    INSUFFICIENT_DATA = "INSUFFICIENT_DATA"


@unique
class AlarmType(str, Enum):
    METRIC = "METRIC"
    COMPOSITE = "COMPOSITE"
    ALL = "ALL"


@unique
class AlarmComparisonOperator(str, Enum):
    GreaterThanOrEqualToThreshold = "GreaterThanOrEqualToThreshold"
    GreaterThanThreshold = "GreaterThanThreshold"
    LessThanThreshold = "LessThanThreshold"
    LessThanOrEqualToThreshold = "LessThanOrEqualToThreshold"
    # FUTURE Anomaly Detector Defs
    # LessThanLowerOrGreaterThanUpperThreshold = "LessThanLowerOrGreaterThanUpperThreshold"
    # LessThanLowerThreshold = "LessThanLowerThreshold"
    # GreaterThanUpperThreshold = "GreaterThanUpperThreshold"


@unique
class AlarmTreatMissingData(str, Enum):
    BREACHING = "breaching"
    NOT_BREACHING = "notBreaching"
    IGNORE = "ignore"
    MISSING = "missing"


class AlarmDefaultAction(ABC):
    @abstractmethod
    def uri(self, alarm_params: Union["AlarmParams", "CompositeAlarmParams"]) -> str:
        ...

    @abstractmethod
    def __eq__(self, other) -> bool:
        ...

    @abstractmethod
    def __ne__(self, other) -> bool:
        ...

    @abstractmethod
    def __hash__(self) -> int:
        ...


class AlarmDefaultActionsMap(CoreData):
    def __init__(
        self,
        ALARM_ACTIONS: Set[AlarmDefaultAction],
        OK_ACTIONS: Optional[Set[AlarmDefaultAction]] = set(),
        INSUFFICIENT_DATA_ACTIONS: Optional[Set[AlarmDefaultAction]] = set(),
    ) -> None:
        self.ALARM_ACTIONS = ALARM_ACTIONS
        self.OK_ACTIONS = OK_ACTIONS
        self.INSUFFICIENT_DATA_ACTIONS = INSUFFICIENT_DATA_ACTIONS


class AlarmParams(CoreData):
    def __init__(
        self,
        target_metric_or_expression: Union["Signal", MetricExpression],
        metric_signals: List["Signal"],
        metric_expressions: List[MetricExpression],
        number_of_evaluation_periods: int,
        number_of_datapoint_periods: int,
        comparison_operator: AlarmComparisonOperator,
        threshold: float,
        default_actions: Optional[AlarmDefaultActionsMap] = None,
        description: Optional[str] = "",
        treat_missing_data: Optional[AlarmTreatMissingData] = AlarmTreatMissingData.MISSING,
    ) -> None:
        self.target_metric_or_expression = target_metric_or_expression
        self.metric_signals = metric_signals
        self.metric_expressions = metric_expressions
        self.number_of_evaluation_periods = number_of_evaluation_periods
        self.number_of_datapoint_periods = number_of_datapoint_periods
        self.comparison_operator = comparison_operator
        self.threshold = threshold
        self.default_actions = default_actions
        self.description = description
        self.treat_missing_data = treat_missing_data

    def check_integrity(self, other: "AlarmParams") -> bool:
        # skip description
        if (
            not isinstance(other, AlarmParams)
            or self.number_of_evaluation_periods != other.number_of_evaluation_periods
            or self.number_of_datapoint_periods != other.number_of_datapoint_periods
            or self.comparison_operator != other.comparison_operator
            or self.threshold != other.threshold
            or self.default_actions != other.default_actions
            or self.treat_missing_data != other.treat_missing_data
            or self.metric_expressions != other.metric_expressions
        ):
            return False

        if type(self.target_metric_or_expression) != type(other.target_metric_or_expression):
            return False

        if isinstance(self.target_metric_or_expression, MetricExpression):
            if self.target_metric_or_expression != other.target_metric_or_expression:
                return False
        else:
            if not self.target_metric_or_expression.check_integrity(other.target_metric_or_expression):
                return False

        if len(self.metric_signals) != len(other.metric_signals):
            return False

        for i, signal in enumerate(self.metric_signals):
            if not signal.check_integrity(other.metric_signals[i]):
                return False

        return True


@unique
class AlarmRuleOperator(Enum):
    OR = 1
    AND = 2


class AlarmRule(CoreData):
    def __init__(
        self, inverted: bool, lhs: Union["AlarmRule", "Signal"], operator: AlarmRuleOperator, rhs: Union["AlarmRule", "Signal"]
    ) -> None:
        self.inverted = inverted
        self.lhs = lhs
        self.operator = operator
        self.rhs = rhs

    def get_alarms(self) -> List["Signal"]:
        alarms = []
        if isinstance(self.lhs, AlarmRule):
            alarms.extend(self.lhs.get_alarms())
        else:
            alarms.append(self.lhs)

        if isinstance(self.rhs, AlarmRule):
            alarms.extend(self.rhs.get_alarms())
        else:
            alarms.append(self.rhs)

        return alarms

    def validate(self) -> None:
        from intelliflow.core.signal_processing import Signal

        if not isinstance(self.lhs, AlarmRule) and not isinstance(self.lhs, Signal):
            raise ValueError(f"Cannot create an AlarmRule from LHS operand with unsupported type {self.lhs!r}")

        if isinstance(self.lhs, Signal) and not self.lhs.type.is_alarm():
            raise ValueError(
                f"Cannot create an AlarmRule using signal {self.lhs.unique_key()!r}! Left operand"
                f" should either be an alarm signal or another AlarmRule."
            )

        if not isinstance(self.rhs, AlarmRule) and not isinstance(self.rhs, Signal):
            raise ValueError(f"Cannot create an AlarmRule from LHS operand with unsupported type {self.rhs!r}")

        if isinstance(self.rhs, Signal) and not self.rhs.type.is_alarm():
            raise ValueError(
                f"Cannot create an AlarmRule using signal {self.rhs.unique_key()!r}! Right operand"
                f" should either be an alarm signal or another AlarmRule."
            )

    def check_integrity(self, other: "AlarmRule") -> bool:
        if not isinstance(other, AlarmRule):
            return False

        if self.inverted != other.inverted or self.operator != other.operator:
            return False

        if type(self.lhs) != type(other.lhs) or type(self.rhs) != type(other.rhs):
            return False

        if not self.lhs.check_integrity(other.lhs) or not self.rhs.check_integrity(other.rhs):
            return False

        return True

    def __invert__(self) -> "AlarmRule":
        """Overwrite bitwise not for high-level convenient operations between AlarmRules and other rules or Signals."""
        return AlarmRule(True, self.lhs, self.operator, self.rhs)

    def __and__(self, other) -> "AlarmRule":
        """Overwrite bitwise & for high-level convenient operations between AlarmRules and other rules or Signals."""
        return self._create_operation(AlarmRuleOperator.AND, other)

    def __or__(self, other) -> "AlarmRule":
        """Overwrite bitwise | for high-level convenient operations between AlarmRules and other rules or Signals."""
        return self._create_operation(AlarmRuleOperator.OR, other)

    def _create_operation(self, operator: AlarmRuleOperator, other) -> "AlarmRule":
        from intelliflow.core.signal_processing.signal import SignalProvider

        return AlarmRule(False, self, operator, other.get_signal() if isinstance(other, SignalProvider) else other)


class CompositeAlarmParams(CoreData):
    def __init__(
        self,
        alarm_rule: Union[AlarmRule, "Signal"],
        default_actions: Optional[AlarmDefaultActionsMap] = AlarmDefaultActionsMap(set()),
        description: Optional[str] = "",
    ) -> None:
        self.alarm_rule = alarm_rule
        self.default_actions = default_actions
        self.description = description

    def check_integrity(self, other: "CompositeAlarmParams") -> bool:
        # skip description
        if (
            not isinstance(other, CompositeAlarmParams)
            or not self.alarm_rule.check_integrity(other.alarm_rule)
            or self.default_actions != other.default_actions
        ):
            return False

        return True
