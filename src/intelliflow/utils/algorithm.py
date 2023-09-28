# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Iterable, Iterator, List


def chunk_iter(iterable: Iterable, chunk_size) -> Iterator[List[Any]]:
    """
    Generates lists of `chunk_size` elements from `iterable`.
    https://stackoverflow.com/a/12797249
    """
    iterable = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(chunk_size):
                chunk.append(next(iterable))
            yield chunk
        except StopIteration:
            if chunk:
                yield chunk
            break
