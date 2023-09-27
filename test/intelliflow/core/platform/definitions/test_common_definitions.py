# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from intelliflow.core.platform.definitions.aws.common import _check_statement_field_equality, normalize_policy_arn


class TestPlatformCommonDefinitions:
    def test_check_statement_field_equality(self):
        assert not _check_statement_field_equality({"Principal": {"Service": "foo"}}, {}, "Principal")
        assert not _check_statement_field_equality({"Principal": {"Service": "foo"}}, {"Principal": {"AWS": "foo"}}, "Principal")
        assert _check_statement_field_equality({"Principal": {"Service": "foo"}}, {"Principal": {"Service": "foo"}}, "Principal")
        assert not _check_statement_field_equality({"Principal": {"Service": "foo"}}, {"Principal": {"Service": "foo"}}, "WRONG_FIELD")
        assert _check_statement_field_equality({"Principal": {"Service": ["foo"]}}, {"Principal": {"Service": "foo"}}, "Principal")
        assert not _check_statement_field_equality(
            {"Principal": {"Service": ["foo", "bar"]}}, {"Principal": {"Service": "foo"}}, "Principal"
        )
        assert not _check_statement_field_equality({"Effect": "Allow"}, {"Effect": "Deny"}, "Principal")
        assert _check_statement_field_equality({"Effect": "Allow"}, {"Effect": "Allow"}, "Effect")
        assert _check_statement_field_equality({"Effect": "Allow"}, {"Effect": ["Allow"]}, "Effect")
        assert not _check_statement_field_equality({"Effect": "Allow"}, {"Effect": ["Allow"]}, "Principal")
        assert _check_statement_field_equality({"Resource": "resource1"}, {"Resource": ["resource1"]}, "Resource")
        assert not _check_statement_field_equality({"Resource": "resource1"}, {"Resource": ["resource1", "resource2"]}, "Resource")

        assert _check_statement_field_equality({"Resource": "resource1"}, {"Resource": ["resource1", "resource1"]}, "Resource")

    def test_normalize_policy_arn(self):
        assert (
            normalize_policy_arn("service-role/AmazonElasticMapReduceEditorsRole")
            == "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceEditorsRole"
        )
        assert (
            normalize_policy_arn("arn:aws:iam::12345678910:policy/CloudRanger/InfoSecHostMonitoringPolicy-DO-NOT-DELETE")
            == "arn:aws:iam::12345678910:policy/CloudRanger/InfoSecHostMonitoringPolicy-DO-NOT-DELETE"
        )
