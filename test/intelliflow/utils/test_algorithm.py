# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from intelliflow.utils.algorithm import chunk_iter


class TestAlgorithm:
    def test_chunk_iter(self):
        assert list(chunk_iter([1, 2, 3, 4, 5, 6], 3)) == [[1, 2, 3], [4, 5, 6]]
        assert list(chunk_iter([1, 2, 3, 4, 5, 6, 7], 3)) == [[1, 2, 3], [4, 5, 6], [7]]
        assert list(chunk_iter([], 3)) == []
        assert list(chunk_iter([1], 3)) == [[1]]
