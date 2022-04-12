# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple, Union, cast
from uuid import uuid4

from intelliflow.core.entity import CoreData
from intelliflow.core.platform.definitions.aws.glue import catalog as glue_catalog
from intelliflow.core.platform.definitions.compute import ComputeSessionStateType
from intelliflow.core.serialization import Serializable, dumps, loads
from intelliflow.core.signal_processing import Slot
from intelliflow.core.signal_processing.dimension_constructs import DimensionSpec, DimensionVariantMapper, DimensionVariantReader
from intelliflow.core.signal_processing.signal import DimensionLinkMatrix, Signal, SignalIntegrityProtocol, SignalLinkNode, SignalUniqueKey
from intelliflow.core.signal_processing.signal_source import SignalSourceType
from intelliflow.core.signal_processing.slot import SlotType

logger = logging.getLogger(__name__)


class _SignalAnalysisResult(CoreData):
    def __init__(self, completed_paths: Set[str], remaining_paths: Set[str]) -> None:
        self.completed_paths = completed_paths
        self.remaining_paths = remaining_paths


class _SignalAnalyzer(ABC):
    """Provides the abstract base for different analyzers so that they can be chained polymorphically or
    different variations can be implemented independently."""

    @classmethod
    @abstractmethod
    def analyze(
        cls,
        signal: Signal,
        platform: "Platform",
        remaining_paths: Optional[Set[str]] = None,
        completed_path_cache: Optional[Set[str]] = None,
    ) -> _SignalAnalysisResult:
        pass


class _SignalRangeAnalyzer(_SignalAnalyzer):
    """Analyzer specialized in checking the readiness/completeness of materialized paths from a Signal."""

    INTERNAL_DATA_MAX_SCAN_RANGE_IN_DAYS = 30

    # overrides
    @classmethod
    def analyze(
        cls,
        signal: Signal,
        platform: "Platform",
        remaining_paths: Optional[Set[str]] = None,
        completed_path_cache: Optional[Set[str]] = None,
    ) -> _SignalAnalysisResult:
        if not (signal.range_check_required or signal.nearest_the_tip_in_range):
            return _SignalAnalysisResult(completed_paths=set(), remaining_paths=set())

        completed_paths: Set[str] = set()
        incomplete_paths: Set[str] = set(remaining_paths) if remaining_paths is not None else None
        if incomplete_paths is None:
            # a brand new analysis request, setup materialized paths
            materialized_paths = signal.get_materialized_resource_paths()
            # for all signals, we start the search from the TIP (if signal is not dependent and have been received
            # already then we should hit this code path, meaning that TIP already be in completed paths)
            # incomplete_paths = set(materialized_paths[0 if is_dependent else 1 :])
            incomplete_paths = set(materialized_paths[0:])

        required_resource_name: Optional[str] = None
        if signal.domain_spec.integrity_check_protocol:
            from intelliflow.core.signal_processing.analysis import INTEGRITY_CHECKER_MAP

            integrity_checker = INTEGRITY_CHECKER_MAP[signal.domain_spec.integrity_check_protocol.type]
            required_resource_name = integrity_checker.get_required_resource_name(
                signal.resource_access_spec, signal.domain_spec.integrity_check_protocol
            )

        for path in incomplete_paths:
            full_path = path
            if required_resource_name:
                full_path = full_path + signal.resource_access_spec.path_delimiter() + required_resource_name
            elif signal.resource_access_spec.path_format_requires_resource_name():
                full_path = full_path + signal.resource_access_spec.path_delimiter() + "_resource"

            try:
                if completed_path_cache and path in completed_path_cache:
                    completed_paths.add(path)
                elif signal.resource_access_spec.source == SignalSourceType.INTERNAL:
                    internal_data_found = False
                    if required_resource_name:  # ex: _SUCCESS file/object
                        # resource_path = resource_path + signal.resource_access_spec.path_delimiter() + required_resource_name
                        # if platform.storage.check_folder(resource_path.lstrip('/')):
                        if platform.storage.check_object([path.lstrip("/")], required_resource_name):
                            completed_paths.add(path)
                            internal_data_found = True
                    elif platform.storage.check_folder(path.lstrip("/")):
                        completed_paths.add(path)
                        internal_data_found = True

                    if not internal_data_found:
                        # SPECIAL handling for pure INLINED executions that don't yield output.
                        # This is also for better testing support.
                        slot_types: Optional[List[SlotType]] = signal.resource_access_spec.slot_types
                        if slot_types and all([slot_type.is_inlined_compute() for slot_type in slot_types]):
                            # try to find the record in routing_table
                            route_id = signal.resource_access_spec.route_id
                            missing_output_signal = signal.create(signal.type, signal.resource_access_spec.source, full_path)
                            inactive_record = platform.routing_table.load_inactive_compute_record(
                                route_id,
                                missing_output_signal,
                                datetime.utcnow() - timedelta(days=cls.INTERNAL_DATA_MAX_SCAN_RANGE_IN_DAYS),
                            )
                            if (
                                inactive_record
                                and inactive_record.session_state
                                and inactive_record.session_state.state_type == ComputeSessionStateType.COMPLETED
                            ):
                                completed_paths.add(path)
                elif signal.resource_access_spec.source in [SignalSourceType.GLUE_TABLE] or (
                    signal.resource_access_spec.proxy and signal.resource_access_spec.proxy.source in [SignalSourceType.GLUE_TABLE]
                ):
                    # do path extraction using the actual spec (not the proxy)
                    dimension_values = signal.resource_access_spec.extract_source(full_path).dimension_values

                    resource_access_spec = signal.resource_access_spec
                    if signal.resource_access_spec.proxy and signal.resource_access_spec.proxy.source in [
                        SignalSourceType.GLUE_TABLE,
                    ]:
                        resource_access_spec = signal.resource_access_spec.proxy

                    database = resource_access_spec.database
                    table = resource_access_spec.table_name

                    session = platform.routing_table.session
                    region = platform.routing_table.region
                    glue = session.client("glue", region_name=region)
                    if glue_catalog.is_partition_present(glue, database, table, [str(d) for d in dimension_values]):
                        completed_paths.add(path)

                elif signal.resource_access_spec.source == SignalSourceType.S3:
                    from intelliflow.core.platform.development import RuntimePlatform
                    from intelliflow.core.platform.definitions.aws.s3.bucket_wrapper import get_bucket
                    from intelliflow.core.platform.definitions.aws.s3.object_wrapper import list_objects, object_exists

                    if isinstance(platform, RuntimePlatform):
                        # at runtime we can comfortably use our own session since execution setup must be done
                        # and connections/permission must have already been established. routing modules' own session, etc
                        # should be able to read from remote data (even if it is upstream).
                        session = platform.routing_table.session
                        region = platform.routing_table.region
                    else:
                        from intelliflow.core.platform.definitions.aws.common import CommonParams as AWSCommonParams

                        # we have to make sure that even when local platform (hence local Processor, RoutingTable) is
                        # being used, we can access upstream data for which we have to use their own session.
                        upstream_platform = platform.routing_table._get_upstream_platform_for(signal, platform)
                        if upstream_platform and signal.resource_access_spec.is_mapped_from_upstream():
                            region = upstream_platform.conf.get_param(AWSCommonParams.REGION)
                            session = upstream_platform.conf.get_param(AWSCommonParams.BOTO_SESSION)
                        else:  # we can use our own session since the external s3 data is marshaled by this app/platform
                            session = platform.routing_table.session
                            region = platform.routing_table.region

                    bucket_name = signal.resource_access_spec.bucket
                    s3 = session.resource("s3", region_name=region)
                    bucket = get_bucket(s3, bucket_name)

                    prefix = path.replace(f"s3://{bucket_name}/", "")
                    # TODO better handling of wild-chars ("*")
                    if required_resource_name:
                        if "*" in prefix:
                            raise ValueError(
                                f"'*' in S3 paths cannot be supported by {cls.__name__!r} when resource "
                                f"name is specified. Signal: {signal.unique_key()!r}, Path: {path!r}"
                            )
                        prefix = prefix + signal.resource_access_spec.path_delimiter() + required_resource_name
                        if object_exists(s3, bucket, prefix):
                            completed_paths.add(path)
                    else:
                        if prefix.endswith("*"):  # e.g 's3://bucket/folder/part*'
                            prefix = prefix[:-1]  # expected state here is like 's3://bucket/folder/part'

                        if "*" in prefix:  # e.g s3://bucket/folder/*/part*
                            raise ValueError(
                                f"'*' in S3 paths cannot be supported by {cls.__name__!r} when it is not "
                                f"used at the end of the path. Signal: {signal.unique_key()!r}, Path: {path!r}"
                            )

                        if len(list(list_objects(bucket, prefix, 1))) >= 1:
                            completed_paths.add(path)
                else:
                    # for new/unknown access spec sources types, default to completed!
                    # rather than failing, let downstream figure out missing data or compute nodes to use signal ranges
                    # at their own risk.
                    completed_paths.add(path)
            except Exception as error:
                logger.critical(
                    f"Exception occurred during the range check for path: {path!r}, error: {error!r}. Will "
                    f"resume the analysis on the same path in the next cycle."
                )

            if completed_paths and signal.nearest_the_tip_in_range:
                # OPTIMIZATION we just care about the completition of at least one path within the range
                # Downstream compute impls (drivers AWS Glue, etc) should use the same flag to pick
                # the 'nearest' resource (e.g partition). So separation of concerns applied here.
                break

        return _SignalAnalysisResult(completed_paths=completed_paths, remaining_paths=incomplete_paths - completed_paths)


def current_timestamp_in_utc() -> int:
    return int(datetime.utcnow().timestamp())


class RuntimeLinkNode(SignalLinkNode):
    def __init__(self, dev_matrix: SignalLinkNode) -> None:
        self._node_id: str = str(uuid4())
        self._activated_timestamp: int = current_timestamp_in_utc()
        self._link_matrix = dev_matrix.link_matrix
        self._signals = dev_matrix.signals
        self._ready_signals: List[Signal] = []
        # maintain the materialized paths of previously processed signals (of ready_signals)
        self._processed_resource_paths: Set[str] = set()
        # maintain previous checkpoints
        self._last_checkpoint_mark: int = None
        # zombie state
        self._is_zombie = False
        # maintain the state of range_check
        self._range_check_complete: bool = not self.range_check_required
        self._range_check_state: Dict[SignalUniqueKey, _SignalAnalysisResult] = {}

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def activated_timestamp(self) -> int:
        return self._activated_timestamp

    @property
    def ready_signals(self) -> Sequence[Signal]:
        return self._ready_signals

    @property
    def last_checkpoint_mark(self) -> int:
        # TODO change after the rollout of hooks
        return getattr(self, "_last_checkpoint_mark", None)

    @last_checkpoint_mark.setter
    def last_checkpoint_mark(self, value: int) -> None:
        self._last_checkpoint_mark = value

    @property
    def is_zombie(self) -> bool:
        return getattr(self, "_is_zombie", False)

    @property
    def range_check_complete(self) -> bool:
        return getattr(self, "_range_check_complete", True)

    @property
    def range_check_state(self) -> Dict[SignalUniqueKey, _SignalAnalysisResult]:
        self._range_check_state = getattr(self, "_range_check_state", {})
        return self._range_check_state

    @property
    def range_check_required(self) -> bool:
        return any([s.range_check_required or s.nearest_the_tip_in_range for s in self._signals])

    def __hash__(self) -> int:
        return hash(self._node_id)

    def __eq__(self, other) -> bool:
        # RuntimeNodes are always unique. Incoming events instantiate new nodes and
        # internally a RuntimeNode does not use pending/ready signals as part of the equality check.
        # At runtime, high-level Route module maintains a dynamic list of runtime nodes based on traffic
        # (history of events received so far).
        return self._node_id == other.node_id

    def auto_complete_ranges(self) -> List[Signal]:
        # TODO post MVP
        # for each ready_signal
        # use SignalSpectralAnalyzer::analyze to do a spectrum, filtered domain, range check.
        #
        # persist the results and use them in subsequent calls to this function, so that it wont return
        # duplicate feedback / reaction back to the Route.
        return []

    def is_ready(self, check_ranges: Optional[bool] = False, platform: Optional["Platform"] = None) -> bool:
        if len(self._signals) != len(self._ready_signals):
            return False

        if self.range_check_required and check_ranges:
            if not self.range_check_complete:
                if not platform:
                    raise ValueError("Platform should be provided to RuntimeLinkNode::is_ready when check_ranges is True!")
                self._range_check_complete = self._check_ranges(platform)

            return self.range_check_complete
        else:
            return True

    def _check_ranges(self, platform: "Platform") -> bool:
        # OPTIMIZATION: it is possible that unique materialized paths can still overlap among different inputs
        # (and their ranges) use this global view of completed (already checked) paths to skip redundant IO.
        common_completed_paths: Set[str] = {
            completed_path for result in self.range_check_state.values() for completed_path in result.completed_paths
        }

        # OPTIMIZATION: we can confidently add the TIP of a non-reference, non-range-check, ready signal
        common_completed_paths.update(
            {
                ready_signal.get_materialized_resource_paths()[0]
                for ready_signal in self._ready_signals
                if not (ready_signal.is_reference or ready_signal.range_check_required or ready_signal.nearest_the_tip_in_range)
            }
        )

        def _update_analysis_result(self, prev_result: _SignalAnalysisResult) -> _SignalAnalysisResult:
            remaining_paths = prev_result.remaining_paths - common_completed_paths if prev_result else None
            # common_completed_paths are sent in (as cache) to avoid unnecessary computation for the first visit.
            result: _SignalAnalysisResult = _SignalRangeAnalyzer.analyze(ready_signal, platform, remaining_paths, common_completed_paths)
            if prev_result:
                result.completed_paths.update(prev_result.completed_paths)
                completed_paths_by_other_analysis = prev_result.remaining_paths - remaining_paths
                result.completed_paths.update(completed_paths_by_other_analysis)
            self.range_check_state[ready_signal.unique_key()] = result
            return result

        for ready_signal in self._ready_signals:
            if ready_signal.range_check_required:
                prev_result: _SignalAnalysisResult = self.range_check_state.get(ready_signal.unique_key(), None)
                result: _SignalAnalysisResult = _update_analysis_result(self, prev_result)
                common_completed_paths.update(result.completed_paths)
                if result.remaining_paths:
                    # OPTIMIZATION: break immediately, no need to waste resources scanning rest.
                    return False
            elif ready_signal.nearest_the_tip_in_range:
                prev_result: _SignalAnalysisResult = self.range_check_state.get(ready_signal.unique_key(), None)
                # check completion condition for nearest check is not True. if any of the completed paths are done, then
                # range-check for this signal is ready.
                if not (prev_result and prev_result.completed_paths):
                    result: _SignalAnalysisResult = _update_analysis_result(self, prev_result)
                    common_completed_paths.update(result.completed_paths)
                    if not result.completed_paths:
                        # this is enough for us to confidently say it is not ready for
                        # nearest_the_tip_in_range = True case.
                        return False

        return True

    def transfer_ranges(self, other_nodes: Set["RuntimeLinkNode"]) -> None:
        """Transfers range_check state from other nodes if they maintain it or merely use the ready signals
        from them."""
        if not self.range_check_required:
            return

        for other_node in other_nodes:
            if other_node.range_check_required:
                for analysis_result in other_node.range_check_state.values():
                    for completed_path in analysis_result.completed_paths:
                        self._update_ranges(completed_path)
            # use the ready_signals (non-ref, no range_check).
            # this type of completed paths might not be contained in the range_check_state, because this
            # signals are not tracked by the range_check_state and we cannot rely on overlap of paths.
            for ready_signal in other_node.ready_signals:
                if not (ready_signal.is_reference or ready_signal.range_check_required or ready_signal.nearest_the_tip_in_range):
                    self._update_ranges(ready_signal)

    def _update_ranges(self, candidate_signal_or_path: Union[Signal, str]) -> None:
        # from a signal only the TIP
        satisfied_path: str = (
            candidate_signal_or_path.get_materialized_resource_paths()[0]
            if isinstance(candidate_signal_or_path, Signal)
            else candidate_signal_or_path
        )
        # check the ready signals in the node
        for ready_signal in self._ready_signals:
            if satisfied_path in ready_signal.get_materialized_resource_paths():
                self._update_signal_range(ready_signal, satisfied_path)

        # check the signals which are (by nature) already materialized and not in the ready_signals yet.
        # very safe operation (nested ifs are just to avoid unnecessary computation), key logic is the check
        # against the materialized_resource_paths using the new 'satisfied_path' from the incoming event (candidate sig)
        for signal in self._signals:
            if signal not in self._ready_signals:  # and signal.domain_spec.dimension_filter_spec.is_material():
                if satisfied_path in signal.get_materialized_resource_paths():
                    self._update_signal_range(signal, satisfied_path)

    def _update_signal_range(self, signal: Signal, satisfied_path: str) -> None:
        if signal.range_check_required or signal.nearest_the_tip_in_range:
            current_analysis_result: _SignalAnalysisResult = self.range_check_state.get(signal.unique_key(), None)
            if current_analysis_result is None:
                current_analysis_result = _SignalAnalysisResult(set(), set(signal.get_materialized_resource_paths()))
            current_analysis_result.remaining_paths.difference_update({satisfied_path})
            current_analysis_result.completed_paths.add(satisfied_path)
            self.range_check_state[signal.unique_key()] = current_analysis_result

    def _is_needed_within_signal_range(self, candidate_signal: Signal, satisfied_path: str) -> bool:
        if candidate_signal.range_check_required or candidate_signal.nearest_the_tip_in_range:
            current_analysis_result: _SignalAnalysisResult = self.range_check_state.get(candidate_signal.unique_key(), None)
            if not current_analysis_result or satisfied_path not in current_analysis_result.completed_paths:
                return True
        return False

    def _check_links(self, candidate_signal: Signal) -> bool:
        if not self._ready_signals:
            return True

        mappers: List[DimensionVariantMapper] = []
        for ready_signal in self._ready_signals:
            signal_mappers = self.get_dim_mappers(ready_signal, candidate_signal)
            if signal_mappers:
                ready_signal.feed(signal_mappers)
                mappers.extend(signal_mappers)

        for mapper in mappers:
            reader = DimensionVariantReader([mapper.target])
            candidate_signal.feed([reader])
            if not reader.variants[mapper.target.name]:
                raise TypeError(f"Candidate signal {candidate_signal.unique_key()} does not have linked dim: {mapper.target}.")
            # check if tips are aligned (sync'ed)
            # Match the top records (Descending).
            # This logic is at the core of RheocerOS Signal group linking.
            #
            # mapped_values of Any type (they are raw).
            # we have to rely on the Variant from the reader to decide on the equality.
            # using reader.values might yield false negatives here.
            if reader.variants[mapper.target.name][0] != mapper.mapped_values[0]:
                return False

        return True

    def _check_dependents(self, output: Signal, output_dim_matrix: DimensionLinkMatrix) -> None:
        """Upon a new ready signal, check if the references can now be materialized from it.

        - First check if output can also be materialized from the ready_signals (hard condition for readiness/trigger).
        - Then, if references' dimension values can be extracted from the ready_signals and the output, then
        add their newly materialized instances to the ready_signals. This will make the node ready.
        """
        if not self._ready_signals or self.is_ready():
            return

        if not self.dependent_signals:
            return

        materialized_output: Signal = None
        try:
            materialized_output = self.materialize_output(output, output_dim_matrix, force=True, allow_partial=True)
        except ValueError as error:
            # since we allow_partial materialization, this should not happen.
            # The scenarios that would yield this should be captured earlier at build time.
            raise RuntimeError(error)

        if materialized_output:
            # output can be fully or partially materialized.
            materialized_inputs = self.get_materialized_inputs_for_output(
                materialized_output,
                output_dim_matrix,
                auto_bind_unlinked_input_dimensions=True,
                already_materialized_inputs=self._ready_signals,
                # ignore if some of the inputs cannot be materialized yet.
                enforce_materialization_on_all_inputs=False,
            )

            if materialized_inputs:
                # we have the materialized version of all of the inputs.
                # we can now transfer materialized references (if any) to ready_signals
                for materialized_input in materialized_inputs:
                    if (
                        materialized_input.is_reference or materialized_input.nearest_the_tip_in_range
                    ) and materialized_input not in self._ready_signals:
                        # We avoid this on references because at this point a reference must be a material ref to fail
                        # link check. If it was not material, then it would not be materialized within
                        # 'get_materialized_inputs_for_output' in the first place, which relies on links already.
                        # if self._check_links(materialized_input):
                        self._ready_signals.append(materialized_input)

    def predict_if_zombie(self, output: Signal, output_dim_matrix: DimensionLinkMatrix) -> False:
        """Going through the already ready signals, estimate if a successful linking will be possible with other inputs.

        We normally have to do it for the first ever ready signal. But the algorithm here can be run at any point before
        the _ready_signals are not fully satisfied (i.e "self.is_ready -> False"), however it would make a difference
        after the first input since the other inputs' compatibility is determined by 'check_links'

        - First check if output can also be materialized from the ready_signals. this is NOT a hard-condition, just to
        relax the subsequent graph search. but if it causes a validation error, then we can easily conclude that the
        incoming signal yields a *zombie* node.
        - Then, if some other inputs have some of their dimensions materialized already, then they might NOT be ok
        with the values on this first signal. So reusing get_materialized_inputs_for_output and checking exceptions
        (validation errors) for linking issues (materialized version of other signals don't match their own filter
        specs) will help use detect *zombie* nodes that will never have fully satisfied linking among its inputs and
        be removed based on TTL.
        """
        if not self._ready_signals or self.is_ready():
            return False

        materialized_output: Signal = None
        try:
            materialized_output = self.materialize_output(output, output_dim_matrix, force=True, allow_partial=True)
            # some of the dimensions of output might not be satisfied/mapped from ready signals yet.
            # but we can still use the partially (or maybe fully logical) output signal and feed it into
            # get_materialized_inputs_for_output it will still be enough to capture TypeErrors (spec incompatibilities).
        except ValueError as error:
            # since we allow_partial materialization, this should not happen.
            # The scenarios that would yield this should be captured earlier at build time.
            raise RuntimeError(error)
        except TypeError:
            # Output's dimension filter rejects the dimension values.
            # Zombie detected! _ready_signals will never be able to yield a ready node.
            # even if we don't have this check and let logical output get into the subsequent call
            # one of the inputs can still reject it if output is linked to them.
            return True

        materialized_inputs: List[Signal] = None
        try:
            materialized_inputs = self.get_materialized_inputs_for_output(
                materialized_output,
                output_dim_matrix,
                auto_bind_unlinked_input_dimensions=True,
                already_materialized_inputs=self._ready_signals,
                enforce_materialization_on_all_inputs=False,
            )
        except TypeError:
            # one of the inputs can be mapped from the _ready_signals but its dimension filter spec rejects it,
            # which means that this node will never pass future _check_links call (once the same input is received).
            # Zombie!
            # Example (in a node with two input signals):
            #     input1 filter spec -> ['*']
            #     input2 filter spec  -> ['NA']
            #
            #     so if input1 is received with ['EU'], it will be added to _ready_signals.
            #     during the zombie check input2 will cause TypeError since its spec ('NA') rejects 'EU'
            #     so there is no point in keeping this node alive because input2 will never be received with 'EU'
            #     by this node since high-level routing will never forward it into the container route of this node.
            return True

        if materialized_inputs is not None and len(materialized_inputs) != len(self._ready_signals):
            # inputs
            # some of the inputs can be auto-materialized from ready signals ( but let's see if they belong to the same
            # link-group. we just need to use _check_links which is normally used against incoming signals.
            for materialized_input in materialized_inputs:
                if materialized_input not in self._ready_signals:
                    if not self._check_links(materialized_input):
                        return True
        return False

    def receive(
        self,
        incoming_signal: Signal,
        output_for_ref_check: Signal = None,
        output_dim_matrix: DimensionLinkMatrix = None,
        update_ranges: bool = True,
    ) -> bool:
        consumed: bool = False

        if set(incoming_signal.get_materialized_resource_paths()).issubset(self._processed_resource_paths):
            consumed = True
        else:
            for s in self._signals:
                if s == incoming_signal:
                    candidate_signal = s.apply(incoming_signal)
                    if candidate_signal:
                        if not (s.domain_spec.dimension_filter_spec and not candidate_signal.domain_spec.dimension_filter_spec):
                            if self._check_links(candidate_signal):
                                if candidate_signal not in self._ready_signals:
                                    self._ready_signals.append(candidate_signal)
                                    # we are not doing proactive garbage-collection of zombies so no need to predict.
                                    self._is_zombie = self.predict_if_zombie(output_for_ref_check, output_dim_matrix)
                                    self._processed_resource_paths.update(incoming_signal.get_materialized_resource_paths())
                                    # self._processed_resource_paths.update(candidate_signal.get_materialized_resource_paths())
                                    if output_for_ref_check and output_dim_matrix:
                                        try:
                                            self._check_dependents(output_for_ref_check, output_dim_matrix)
                                        except TypeError:
                                            self._is_zombie = True
                                    consumed = True
                                elif candidate_signal.nearest_the_tip_in_range:
                                    # check if signal was previously auto-added to _ready_signals via _check_dependents.
                                    # if so and also if it satisfies the TIP condition (satisfied_path should point to
                                    # TIP because _check_links succeeded), then we should consume (otherwise, route
                                    # would create a new pending node).
                                    satisfied_path: str = candidate_signal.get_materialized_resource_paths()[0]
                                    if self._is_needed_within_signal_range(candidate_signal, satisfied_path):
                                        consumed = True
                            if update_ranges:
                                self._update_ranges(candidate_signal)
                        else:
                            # this happens when the incoming signal matches the same input with different alias'
                            # let's be opportunistic and check if it is zombie using the links between those inputs.
                            # ex:
                            #   'alias1': input['*']
                            #   'alias2': input['NA']
                            #    and assume that auto-linking is enabled on this single dimension.
                            #
                            #    incoming_signal -> input['EU']
                            try:
                                self._check_links(candidate_signal)
                            except TypeError:
                                self._is_zombie = True
        return consumed

    def materialize_output(
        self, dev_output: Signal, dim_links: DimensionLinkMatrix, force: bool = False, allow_partial: bool = False
    ) -> Optional[Signal]:
        """Create/materialize the final output Signal that would map to a concrete resource (ex: dataset partition).

        This expects the link group inputs to be satisfied / ready.

        Using the `dim_links` to map dimension values from the inputs, it scans them and chooses
        the most recent/highest ranking value (if there is a range) and assigns it to output's dimension.
        """
        if not force and not self.is_ready():
            return None

        mappers: List[DimensionVariantMapper] = []
        for ready_signal in self._ready_signals:
            signal_mappers = self._get_dim_mappers(ready_signal, dev_output, dim_links)
            if signal_mappers:
                ready_signal.feed(signal_mappers)
                # TODO we don't need to enforce this. If inputs don't have the link and we have multiple eligible values
                # for a dim, then our output can have multiple materialized paths.
                # But the key problem is to avoid ranges and still be able to get the tip (link point).
                # So dimensionvariant sub-types (relativevariant) should provide this hint.
                for mapper in signal_mappers:
                    # if multiple_values are mapped from a linked source (ex: date range, etc)
                    # choose the most recent, highest, etc.
                    # Mapper will use materialized value while mapping the output Signal.
                    sorted_mapped_values = list(mapper.mapped_values)
                    # sorted_mapped_values.sort(reverse=True)
                    mapper.set_materialized_value(sorted_mapped_values[0])

                mappers.extend(signal_mappers)

        # now see if the output has any literal assignments to its dimensions
        literal_value_mappers = self._get_dim_mappers(None, None, dim_links)
        if literal_value_mappers:
            for mapper in literal_value_mappers:
                if len(mapper.source) > 1:
                    raise ValueError(
                        f"Cannot assign multiple literal values to output dimension! Output: " f"{dev_output.alias!r}, Mapper: {mapper!r}"
                    )
                mapper.read(cast("DimensionVariant", mapper.source[0]))
            mappers.extend(literal_value_mappers)

        # TODO create mappers for its own materialized dimensions

        return dev_output.materialize(mappers, allow_partial=allow_partial)


RouteID = str


class RouteCheckpoint(CoreData):
    def __init__(self, checkpoint_in_secs: int, slot: Slot) -> None:
        self.checkpoint_in_secs = checkpoint_in_secs
        self.slot = slot

    def check_integrity(self, other: "RouteCheckpoint") -> bool:
        if not other:
            return False

        if self.checkpoint_in_secs != other.checkpoint_in_secs:
            return False

        if not self.slot.check_integrity(other.slot):
            return False

        return True


class RouteExecutionHook(CoreData):
    def __init__(
        self,
        # execution initiation hooks
        on_exec_begin: Optional[Slot] = None,
        on_exec_skipped: Optional[Slot] = None,
        # an execution might have multiple computes (inlined or batch)
        # high granularity tracking
        on_compute_success: Optional[Slot] = None,
        on_compute_failure: Optional[Slot] = None,
        on_compute_retry: Optional[Slot] = None,
        # high level (execution context level) hooks
        on_success: Optional[Slot] = None,
        on_failure: Optional[Slot] = None,
        # hooks for custom checkpoints across the execution timeline
        checkpoints: Optional[List[RouteCheckpoint]] = None,
    ) -> None:
        self.on_exec_begin = on_exec_begin
        self.on_exec_skipped = on_exec_skipped
        # an execution might have multiple computes (inlined or batch)
        # high granularity tracking
        self.on_compute_success = on_compute_success
        self.on_compute_failure = on_compute_failure
        self.on_compute_retry = on_compute_retry
        # high level (execution context level) hooks
        self.on_success = on_success
        self.on_failure = on_failure
        # hooks for custom checkpoints across the execution timeline
        self.checkpoints = checkpoints

    def callbacks(self) -> Tuple[Slot]:
        callbacks = (
            self.on_exec_begin,
            self.on_exec_skipped,
            self.on_compute_success,
            self.on_compute_failure,
            self.on_compute_retry,
            self.on_success,
            self.on_failure,
        )
        if self.checkpoints:
            callbacks = callbacks + tuple([checkpoint.slot for checkpoint in self.checkpoints])
        return callbacks

    def check_integrity(self, other: "RouteExecutionHook") -> bool:
        if not other:
            return False

        if self.on_exec_begin or other.on_exec_begin:
            if (not self.on_exec_begin and other.on_exec_begin) or not self.on_exec_begin.check_integrity(other.on_exec_begin):
                return False

        if self.on_exec_skipped or other.on_exec_skipped:
            if (not self.on_exec_skipped and other.on_exec_skipped) or not self.on_exec_skipped.check_integrity(other.on_exec_skipped):
                return False

        if self.on_compute_success or other.on_exec_skipped:
            if (not self.on_compute_success and other.on_compute_success) or not self.on_compute_success.check_integrity(
                other.on_compute_success
            ):
                return False

        if self.on_compute_failure or other.on_compute_failure:
            if (not self.on_compute_failure and other.on_compute_failure) or not self.on_compute_failure.check_integrity(
                other.on_compute_failure
            ):
                return False

        if self.on_compute_retry or other.on_compute_retry:
            if (not self.on_compute_retry and other.on_compute_retry) or not self.on_compute_retry.check_integrity(other.on_compute_retry):
                return False

        if self.on_success or other.on_success:
            if (not self.on_success and other.on_success) or not self.on_success.check_integrity(other.on_success):
                return False

        if self.on_failure or other.on_failure:
            if (not self.on_failure and other.on_failure) or not self.on_failure.check_integrity(other.on_failure):
                return False

        if (self.checkpoints is None and other.checkpoints) or (self.checkpoints and other.checkpoints is None):
            return False
        elif self.checkpoints and other.checkpoints:
            if len(self.checkpoints) != len(other.checkpoints):
                return False

            for i, checkpoint in enumerate(self.checkpoints):
                if not checkpoint.check_integrity(other.checkpoints[i]):
                    return False

        return True

    def chain(self, *other: "RouteExecutionHook") -> "RouteExecutionHook":
        checkpoints = []
        execution_hook = [self]
        execution_hook.extend(other)
        for hook in execution_hook:
            if hook.checkpoints:
                for c in hook.checkpoints:
                    checkpoints.append(c)

        return RouteExecutionHook(
            on_exec_begin=_ChainedHookCallback("on_exec_begin", *execution_hook),
            on_exec_skipped=_ChainedHookCallback("on_exec_skipped", *execution_hook),
            on_compute_success=_ChainedHookCallback("on_compute_success", *execution_hook),
            on_compute_failure=_ChainedHookCallback("on_compute_failure", *execution_hook),
            on_compute_retry=_ChainedHookCallback("on_compute_retry", *execution_hook),
            on_success=_ChainedHookCallback("on_success", *execution_hook),
            on_failure=_ChainedHookCallback("on_failure", *execution_hook),
            checkpoints=checkpoints,
        )


class RoutePendingNodeHook(CoreData):
    def __init__(
        self,
        on_pending_node_created: Optional[Slot] = None,
        # when a pending node expires (based on route TTL)
        on_expiration: Optional[Slot] = None,
        checkpoints: Optional[List[RouteCheckpoint]] = None,
    ) -> None:
        self.on_pending_node_created = on_pending_node_created
        # when a pending node expires (based on route TTL)
        self.on_expiration = on_expiration
        self.checkpoints = checkpoints

    def callbacks(self) -> Tuple[Slot]:
        callbacks = (self.on_pending_node_created, self.on_expiration)
        if self.checkpoints:
            callbacks = callbacks + tuple([checkpoint.slot for checkpoint in self.checkpoints])
        return callbacks

    def check_integrity(self, other: "RoutePendingNodeHook") -> bool:
        if not other:
            return False

        if self.on_pending_node_created or other.on_pending_node_created:
            if (not self.on_pending_node_created and other.on_pending_node_created) or not self.on_pending_node_created.check_integrity(
                other.on_pending_node_created
            ):
                return False

        if self.on_expiration or other.on_expiration:
            if (not self.on_expiration and other.on_expiration) or not self.on_expiration.check_integrity(other.on_expiration):
                return False

        if (self.checkpoints is None and other.checkpoints) or (self.checkpoints and other.checkpoints is None):
            return False
        elif self.checkpoints and other.checkpoints:
            if len(self.checkpoints) != len(other.checkpoints):
                return False

            for i, checkpoint in enumerate(self.checkpoints):
                if not checkpoint.check_integrity(other.checkpoints[i]):
                    return False

        return True

    def chain(self, *other: "RoutePendingNodeHook") -> "RoutePendingNodeHook":
        checkpoints = []
        pending_node_hook = [self]
        pending_node_hook.extend(other)
        for hook in pending_node_hook:
            if hook.checkpoints:
                for c in hook.checkpoints:
                    checkpoints.append(c)

        return RoutePendingNodeHook(
            on_pending_node_created=_ChainedHookCallback("on_pending_node_created", *pending_node_hook),
            on_expiration=_ChainedHookCallback("on_expiration", *pending_node_hook),
            checkpoints=checkpoints,
        )


class _ChainedHookCallback(Callable):
    def __init__(self, callback: str, *hook: Union[RouteExecutionHook, RoutePendingNodeHook]):
        self._hooks = list(hook)
        self._callback = callback

    def __call__(self, *args, **kwargs):
        for hook in self._hooks:
            callback = getattr(hook, self._callback)
            if callback:
                if isinstance(callback, Slot):
                    slot = callback
                    code = loads(slot.code)
                    if isinstance(code, str):
                        exec(code)
                    else:
                        code(*args, **kwargs)
                else:
                    callback(*args, **kwargs)


class RoutingSession(CoreData):
    def __init__(self, max_duration_in_secs: Optional[int] = None) -> None:
        self.start_time_in_utc = current_timestamp_in_utc()
        self.max_duration_in_secs = max_duration_in_secs

    def is_expired(self) -> bool:
        if self.max_duration_in_secs is not None:
            return (current_timestamp_in_utc() - self.start_time_in_utc) > self.max_duration_in_secs
        return False


class Route(Serializable):
    class ExecutionContext(CoreData):
        def __init__(self, completed_link_node: RuntimeLinkNode, output: Signal, slots: List[Slot]) -> None:
            self.completed_link_node = completed_link_node
            self.output = output
            self.slots = slots

        @property
        def id(self) -> str:
            return self.completed_link_node.node_id

        def __hash__(self) -> int:
            return hash(self.id)

    class Response(CoreData):
        # reactions are sent back to main loop for new cycles.
        # Ex: backfilling requests are reactions
        def __init__(
            self, reactions: List[Signal], new_execution_contexts: List["Route.ExecutionContext"], new_pending_nodes: Set[RuntimeLinkNode]
        ) -> None:
            self.reactions = reactions
            self.new_execution_contexts = new_execution_contexts
            self.new_pending_nodes = new_pending_nodes

    def __init__(
        self,
        route_id: RouteID,
        link_node: SignalLinkNode,
        output: Signal,
        output_dim_matrix: DimensionLinkMatrix,
        slots: Sequence[Slot],
        auto_range_completion=False,
        execution_hook: RouteExecutionHook = None,
        pending_node_ttl_in_secs: int = None,
        pending_node_hook: RoutePendingNodeHook = None,
    ) -> None:
        self._id = route_id if route_id else str(uuid4())
        self._output = output
        self._output_dim_matrix = output_dim_matrix
        self._link_node = link_node
        self._slots = list(slots)
        self._execution_hook = execution_hook if execution_hook else RouteExecutionHook()
        self._pending_node_hook = pending_node_hook if pending_node_hook else RoutePendingNodeHook()
        self._pending_node_ttl_in_secs = pending_node_ttl_in_secs
        # TODO / FUTURE auto range check and responding with reactions will effect RuntimeLinkNode impl as well.
        self._auto_range_completion = auto_range_completion
        self._pending_nodes: Set[RuntimeLinkNode] = set()

        if self._execution_hook:
            if self._execution_hook.checkpoints:
                self._execution_hook.checkpoints.sort(key=lambda check_point: check_point.checkpoint_in_secs)

        if self._pending_node_hook:
            if not self._pending_node_ttl_in_secs and self._pending_node_hook.on_expiration:
                raise ValueError(f"Pending node expiration hook is defined without a TTL.")
            if self._pending_node_hook.checkpoints:
                self._pending_node_hook.checkpoints.sort(key=lambda check_point: check_point.checkpoint_in_secs)

    def has_execution_checkpoints(self) -> bool:
        return bool(self.execution_hook and self.execution_hook.checkpoints)

    def has_pending_node_checkpoints(self) -> bool:
        return bool(self.pending_node_hook and self.pending_node_hook.checkpoints)

    def get_next_execution_checkpoints(
        self, elapsed_time: int, previous_checkpoint: Optional[int] = None
    ) -> Optional[List[RouteCheckpoint]]:
        """Return chronologically ordered list of execution checkpoints between the 'previous_checkpoint'
        and the new 'elapsed_time."""
        if self.execution_hook and self.execution_hook.checkpoints:
            return self._get_next_checkpoints(elapsed_time, self.execution_hook.checkpoints, previous_checkpoint)

    def get_next_pending_node_checkpoint(
        self, elapsed_time: int, previous_checkpoint: Optional[int] = None
    ) -> Optional[List[RouteCheckpoint]]:
        """Return chronologically ordered list of pending node checkpoints between the 'previous_checkpoint'
        and 'elapsed_time."""
        if self.pending_node_hook and self.pending_node_hook.checkpoints:
            return self._get_next_checkpoints(elapsed_time, self.pending_node_hook.checkpoints, previous_checkpoint)

    @classmethod
    def _get_next_checkpoints(
        cls, elapsed_time: int, checkpoints: List[RouteCheckpoint], previous_checkpoint: Optional[int] = None
    ) -> List[RouteCheckpoint]:
        next_checkpoints = []
        for checkpoint in reversed(checkpoints):
            if elapsed_time >= checkpoint.checkpoint_in_secs and (
                not previous_checkpoint or checkpoint.checkpoint_in_secs > previous_checkpoint
            ):
                next_checkpoints.append(checkpoint)
        return next_checkpoints

    @property
    def route_id(self) -> RouteID:
        return self._id

    @property
    def link_node(self) -> SignalLinkNode:
        return self._link_node

    @property
    def output(self) -> Signal:
        return self._output

    @property
    def output_dim_matrix(self) -> DimensionLinkMatrix:
        return self._output_dim_matrix

    @property
    def slots(self) -> Sequence[Slot]:
        return self._slots

    @property
    def pending_nodes(self) -> Set[RuntimeLinkNode]:
        return self._pending_nodes

    @property
    def execution_hook(self) -> RouteExecutionHook:
        return getattr(self, "_execution_hook", RouteExecutionHook())

    @execution_hook.setter
    def execution_hook(self, value) -> None:
        self._execution_hook = value

    @property
    def pending_node_hook(self) -> RoutePendingNodeHook:
        return getattr(self, "_pending_node_hook", RoutePendingNodeHook())

    @pending_node_hook.setter
    def pending_node_hook(self, value) -> None:
        self._pending_node_hook = value

    @property
    def pending_node_ttl_in_secs(self) -> int:
        return getattr(self, "_pending_node_ttl_in_secs", None)

    @pending_node_ttl_in_secs.setter
    def pending_node_ttl_in_secs(self, value) -> None:
        self._pending_node_ttl_in_secs = value

    def clone(self) -> "Route":
        return copy.deepcopy(self)

    def __hash__(self) -> int:
        return hash(self._id)

    def __eq__(self, other: "Route") -> bool:
        return self._id == other._id
        # return self._link_node == other.link_node and self._output == other.output

    def check_integrity(self, other: "Route") -> bool:
        """Checks internal input, output, slot and detects data-integrity, configuration changes"""
        if self != other:
            raise ValueError(f"Cannot run integrity-check on a different route! Source: {self.route_id}, Target: {other.route_id} ")

        if not self.link_node.check_integrity(other.link_node):
            return False

        if not self.output.check_integrity(other.output):
            return False

        if len(self._output_dim_matrix) != len(other._output_dim_matrix):
            return False

        for link in self._output_dim_matrix:
            if not any(link.check_integrity(other_link) for other_link in other._output_dim_matrix):
                return False

        if len(self.slots) != len(other.slots):
            return False

        for i, slot in enumerate(self.slots):
            if not slot.check_integrity(other.slots[i]):
                return False

        # TODO check hooks and pending node TTL

        return True

    def check_auxiliary_data_integrity(self, other: "Route") -> bool:
        """Checks the difference in execution and pending node hooks.
        The reason hooks are not as part of the integrity check: they would still need state transfer in the routing
        module but the change in hooks should not invalidate the current state of a route, or mean incompatibility in
        any way.
        So the return value of this method would indicate (for example) a swap of hook state/values between these two
        routes.

        returns False if a change is detected.
        """
        if self != other:
            raise ValueError(f"Cannot run hook state diff check on a different route! Source: {self.route_id}, Target: {other.route_id} ")

        if (not self.execution_hook and other.execution_hook) or not self.execution_hook.check_integrity(other.execution_hook):
            return False

        if (not self.pending_node_hook and other.pending_node_hook) or not self.pending_node_hook.check_integrity(other.pending_node_hook):
            return False

        if self.pending_node_ttl_in_secs != other.pending_node_ttl_in_secs:
            return False

        return True

    def transfer_auxiliary_data(self, other: "Route") -> None:
        self.execution_hook = other.execution_hook
        self.pending_node_hook = other.pending_node_hook
        self.pending_node_ttl_in_secs = other.pending_node_ttl_in_secs

    def receive(self, incoming_signal: Signal, platform: Optional["Platform"] = None) -> Optional[Response]:
        if not self._link_node.can_receive(incoming_signal):
            return None

        has_consumed: bool = False
        if self._pending_nodes:
            for pending_node in self._pending_nodes:
                if pending_node.receive(incoming_signal, self._output, self._output_dim_matrix):
                    has_consumed = True
                    # keep iterating since multiple nodes/windows might be active on the same Route.

        new_nodes: Set[RuntimeLinkNode] = set()
        if not has_consumed:
            # OPTIMIZATION: a ref signal *should* not create a pending node as it can be materialized from others.
            #               So the non-references are the "determinants" for a node and let's wait for them.
            #               we don't return immediately and go over the pending nodes because incoming ref might have
            #               satisfied a path in any of them and made them 'ready'.
            if not incoming_signal.is_reference:
                new_node = RuntimeLinkNode(self._link_node)
                if new_node.receive(incoming_signal, self._output, self._output_dim_matrix):
                    # we just need to do this transfer once here, then node itself will keep it self up to date.
                    new_node.transfer_ranges(self._pending_nodes)
                    self._pending_nodes.add(new_node)
                    new_nodes.add(new_node)
                else:  # no further action required. this route has nothing to do with this signal
                    return None

        new_reactions: List[Signal] = []
        new_executions: List[Route.ExecutionContext] = []
        completed_nodes: Set[RuntimeLinkNode] = set()
        for pending_node in self._pending_nodes:
            if pending_node.is_ready(check_ranges=True, platform=platform):
                new_nodes.discard(pending_node)  # it might have been just added, remove from 'new' pending list.
                output: Signal = pending_node.materialize_output(self._output, self._output_dim_matrix)
                #  We are not checking if output is ready.
                #  Currently we think that routing module should be agnostic from this.
                #  This is different than range_completion for which this module is the only place
                #  to resolve missing dimension ranges and react accordingly in a stateless manner.
                completed_nodes.add(pending_node)
                new_executions.append(Route.ExecutionContext(pending_node, output, self._slots))
            elif self._auto_range_completion:
                # TODO if range_completion is enabled and incoming signal is not already a reaction
                # then initiate a range completion process as a series of feedback Signals back to the main
                # dispatcher loop.
                new_reactions.extend(pending_node.auto_complete_ranges())

        self._pending_nodes = self._pending_nodes - completed_nodes

        return Route.Response(new_reactions, new_executions, new_nodes)

    def check_expired_nodes(self) -> Set[RuntimeLinkNode]:
        """Expiration implementation for a Route.
        Checks all of the pending nodes and uses the TTL value to expire them.

        Modifies the internal state of Route and returns the expired nodes.
        """
        expired_nodes: Set[RuntimeLinkNode] = set()
        if self.pending_node_ttl_in_secs:
            for pending_node in self._pending_nodes:
                elapsed_time = current_timestamp_in_utc() - pending_node.activated_timestamp
                if elapsed_time >= self.pending_node_ttl_in_secs:
                    expired_nodes.add(pending_node)
            self._pending_nodes = self._pending_nodes - expired_nodes
        return expired_nodes

    def check_new_ready_pending_nodes(self, platform: "Platform", routing_session: RoutingSession) -> Response:
        """Checks all of the pending nodes and see if there are new 'ready' ones.

        New ready nodes can pop up because of 'range_check's on nodes. Other than
        that, a state change into 'ready' for a node can happen only after an incoming
        signal (see Route::receive).

        Modifies the internal state of Route and returns a new Route::Response similar
        to "Route::receive".
        """
        new_executions: List[Route.ExecutionContext] = []
        completed_nodes: Set[RuntimeLinkNode] = set()
        for pending_node in self._pending_nodes:
            if routing_session.is_expired():
                break
            if pending_node.is_ready(check_ranges=True, platform=platform):
                output: Signal = pending_node.materialize_output(self._output, self._output_dim_matrix)
                completed_nodes.add(pending_node)
                new_executions.append(Route.ExecutionContext(pending_node, output, self._slots))

        self._pending_nodes = self._pending_nodes - completed_nodes

        return Route.Response([], new_executions, set())
