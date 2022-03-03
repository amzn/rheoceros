# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from intelliflow.core.platform.definitions.aws.common import _check_statement_field_equality


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
