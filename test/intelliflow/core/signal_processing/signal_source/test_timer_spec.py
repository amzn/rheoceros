# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from test.intelliflow.core.signal_processing.signal_source.test_internal_data_spec import TestInternalDataAccessSpec

import pytest

from intelliflow.core.serialization import dumps, loads
from intelliflow.core.signal_processing.signal_source import *


class TestTimerAccessSpec:
    timer_access_spec = TimerSignalSourceAccessSpec("test_timer", "rate(1 day)", "my_application")
    timer_access_spec_cloned = copy.deepcopy(timer_access_spec)

    timer_access_spec_attrs = TimerSignalSourceAccessSpec("test_timer", "rate(1 day)", "my_application", dummy_key="dummy_value")

    timer_access_spec_with_cron_exp = TimerSignalSourceAccessSpec("test_timer", "cron(* * * * *)", "my_application")

    def test_timer_signal_source_access_spec_api(self):
        assert (
            self.timer_access_spec.timer_id == self.timer_access_spec_cloned.timer_id
            and self.timer_access_spec.schedule_expression == self.timer_access_spec_cloned.schedule_expression
            and self.timer_access_spec_cloned.context_id == self.timer_access_spec_cloned.context_id
        )

        assert self.timer_access_spec.source == SignalSourceType.TIMER
        assert (
            self.timer_access_spec.path_format
            == f"my_application-test_timer{TimerSignalSourceAccessSpec.path_delimiter()}{DIMENSION_PLACEHOLDER_FORMAT}"
        )

    def test_timer_signal_source_access_spec_equality(self):
        assert self.timer_access_spec == self.timer_access_spec_cloned

    @pytest.mark.parametrize(
        "timer_id, schedule_exp, context_id, attrs",
        [
            ("test_timer1", "rate(1 day)", "my_application", {}),
            ("test_timer", "rate(2 days)", "my_application", {}),
            ("test_timer", "rate(1 day)", "my_application1", {}),
            ("test_timer", "rate(1 day)", "my_application", {"dummy_key": "dummy_value"}),
        ],
    )
    def test_timer_signal_source_access_spec_inequality(self, timer_id, schedule_exp, context_id, attrs):
        assert self.timer_access_spec != TimerSignalSourceAccessSpec(timer_id, schedule_exp, context_id, **attrs)

    def test_timer_signal_source_access_spec_serialization(self):
        assert self.timer_access_spec == loads(dumps(self.timer_access_spec))

    def test_timer_signal_source_access_spec_integrity(self):
        assert self.timer_access_spec.check_integrity(self.timer_access_spec_cloned)
        # attrs will be ignored during the check
        assert self.timer_access_spec.check_integrity(self.timer_access_spec_attrs)
        # schedule_exp not part of the path_format, so should be checked separately
        assert not self.timer_access_spec.check_integrity(self.timer_access_spec_with_cron_exp)
        assert not self.timer_access_spec.check_integrity(TestInternalDataAccessSpec.internal_data_access_spec)

    def test_timer_signal_source_access_spec_path_creation(self):
        assert TimerSignalSourceAccessSpec.create_resource_path("my_application-my_timer", "2020-05-16")

    def test_timer_signal_source_access_spec_extract_time(self):
        assert TimerSignalSourceAccessSpec.extract_time("my_application-my_timer/2020-05-15/timer") == "2020-05-15"
        assert TimerSignalSourceAccessSpec.extract_time("my_application-my_timer/2020-05-15") == "2020-05-15"
