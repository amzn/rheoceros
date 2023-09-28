# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from urllib.parse import urlparse

from intelliflow.utils.url_validation import validate_url


class TestUrlValidation:
    def test_validate_url(self):
        assert validate_url("http://google.com")
        assert not validate_url("http://127.0.0.1")
        assert not validate_url("http://169.254.169.254:80")
        assert not validate_url("http://localhost:80")
        assert not validate_url("http://0x7f.0x0.0x0.0x1")
