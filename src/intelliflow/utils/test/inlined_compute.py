# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

# helps modules that won't be as part of the deployed bundle to use InlinedCompute.
# ex: test, test_integration, etc.
import uuid
from typing import Any, ClassVar, Dict

from intelliflow.api import IInlinedCompute, InlinedCompute
from intelliflow.core.platform.constructs import ConstructParamsDict
from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams
from intelliflow.core.platform.definitions.aws.s3.bucket_wrapper import get_bucket
from intelliflow.core.platform.definitions.aws.s3.object_wrapper import get_object, object_exists, put_object
from intelliflow.core.platform.definitions.compute import ComputeRetryableInternalError
from intelliflow.core.signal_processing import Signal

NOOPCompute = InlinedCompute(lambda input_map, output, params: ...)


# Inlined compute definitions for easy chaos testing of orchestration/routing


def RandomTimedNOOPCompute(max_sleep_in_secs: int) -> InlinedCompute:
    return InlinedCompute(_RandomTimedNOOPCompute(max_sleep_in_secs))


class _RandomTimedNOOPCompute(IInlinedCompute):
    def __init__(self, max_sleep_in_secs: int) -> None:
        assert max_sleep_in_secs > 0, f"Please provide a positive integer for max_sleep_in_secs parameter of {self.__class__.__name__}"
        self._max_sleep_in_secs = max_sleep_in_secs

    def __call__(self, input_map: Dict[str, Signal], materialized_output: Signal, params: ConstructParamsDict) -> Any:
        from random import randrange

        randrange(0, self._max_sleep_in_secs)


FailureCompute = InlinedCompute(
    lambda input_map, output, params: (_ for _ in ()).throw(RuntimeError("Synthetic failure by FailureCompute!"))
)


def RandomFailureCompute(probability: float) -> InlinedCompute:
    return InlinedCompute(_RandomFailureCompute(probability))


class _RandomFailureCompute(IInlinedCompute):
    def __init__(self, probability: float) -> None:
        assert probability > 0, f"Please provide a positive float value for probability parameter of {self.__class__.__name__}"
        self._probability = probability

    def __call__(self, input_map: Dict[str, Signal], materialized_output: Signal, params: ConstructParamsDict) -> Any:
        from random import uniform

        if uniform(0, 1.0) < self._probability:
            raise RuntimeError(f"Synthetic random failure by RandomFailureCompure(probability={self._probability})")


class InlinedComputeRetryVerifier(IInlinedCompute):

    FOLDER: ClassVar[str] = "test_inlined_compute_data"

    def __init__(
        self,
        retry_count: int,
        # TODO remove after IInlinedCompute interface change (to add routing_table)
        storage_bucket: str,
    ):
        self._id = str(uuid.uuid4())
        self._retry_count = retry_count
        self._storage_bucket = storage_bucket

    def __call__(self, input_map: Dict[str, Signal], materialized_output: Signal, params: ConstructParamsDict) -> Any:
        # TODO remove when IInlinedCompute interface includes routing_table which would help us use platform::Storage
        #  generically for the persistence of retry state
        if AWSCommonParams.BOTO_SESSION not in params:
            raise NotImplementedError(f"{self.__class__.__name__} currently supports AWS only!")

        assert params.get("dimensions", None) is not None, "Output 'dimensions' map not in InlinedCompute params"
        assert params.get("dimensions_map", None) is not None, "Output 'dimensions_map' map not in InlinedCompute params"

        s3 = params[AWSCommonParams.BOTO_SESSION].resource("s3")
        bucket = get_bucket(s3, self._storage_bucket)
        retry_count_key = self.FOLDER + "/" + self._id + "/current_retry_count"
        if object_exists(s3, bucket, retry_count_key):
            current_retry_count = int(get_object(bucket, retry_count_key))
        else:
            current_retry_count = -1  # first execution does not count towards retry_count

        if current_retry_count == self._retry_count:
            raise RuntimeError(
                f"RheocerOS called the inlined compute redundantly despite that previous execution"
                f" was successful after {self._retry_count} attempt{'s' if self._retry_count > 1 else ''}"
            )

        current_retry_count = current_retry_count + 1
        put_object(bucket, retry_count_key, str(current_retry_count).encode("utf-8"))
        if current_retry_count < self._retry_count:
            raise ComputeRetryableInternalError("Retry me Retry me Retry me")
        else:
            # now desired # of retries is reached, now we can let IF to complete/finalize this compute/execution.
            # if IF attempts to call this compute again, then the if statement above checking current_retry_count against
            # retry_count will raise RuntimeError and this will be easy to catch in the tests using this verifier.
            pass

    def verify(self, app: "Application") -> bool:
        s3 = app.platform.conf.get_param(AWSCommonParams.BOTO_SESSION).resource("s3")
        bucket = get_bucket(s3, self._storage_bucket)
        retry_count_key = self.FOLDER + "/" + self._id + "/current_retry_count"
        if object_exists(s3, bucket, retry_count_key):
            current_retry_count = int(get_object(bucket, retry_count_key))
        else:
            raise RuntimeError(
                f"Computation could not be detected, {retry_count_key!r} was not persisted in " f" Storage bucket {self._storage_bucket!r}"
            )

        return current_retry_count == self._retry_count
