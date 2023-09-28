# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import base64
import hashlib
from typing import Optional


def calculate_bytes_sha256(input: bytes) -> Optional[str]:
    """
    Calculate SHA256 of of a byte array as Base64 encoded string.
    :param input: byte array of input
    :return base64 encoded SHA256 hash of the bytes
    """
    hasher = hashlib.sha256()
    hasher.update(input)
    return base64.b64encode(hasher.digest()).decode("ascii")
