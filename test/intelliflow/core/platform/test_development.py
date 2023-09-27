# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from unittest.mock import MagicMock

import intelliflow.core.platform.development as development
from intelliflow.core.platform.development import AWSConfiguration


class TestDevelopment:
    def test_setup_session(self):
        config = (
            AWSConfiguration.builder()
            .with_default_credentials(as_admin=True)
            .with_region("us-east-1")
            .build()
        )

        _real_get_session = development.get_session

        def mock_get_session(credential_config, region, duration=0):
            return MagicMock()

        development.get_session = MagicMock(side_effect=mock_get_session)

        needs_to_assume_role, needs_to_check_role, session = config._setup_session("devRole")
        assert needs_to_assume_role
        assert needs_to_check_role
        assert session
