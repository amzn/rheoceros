# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import uuid
from typing import ClassVar

from intelliflow.api import IExecutionBeginHook
from intelliflow.core.platform.compute_targets.descriptor import ComputeDescriptor
from intelliflow.core.platform.definitions.compute import ComputeInternalError
from intelliflow.core.serialization import dumps
from intelliflow.core.signal_processing import Signal, Slot


class HookBase:
    FOLDER: ClassVar[str] = "hook_test_folder"

    def __init__(self):
        self._expected_data = str(uuid.uuid4())

    def _save(self, routing_table: "RoutingTable"):
        routing_table.get_platform().storage.save(self._expected_data, [self.FOLDER], f"{self._expected_data}_expected_data")

    def verify(self, app: "Application") -> bool:
        key: str = f"{self._expected_data}_expected_data"
        return app.platform.storage.check_object([self.FOLDER], key) and self._expected_data == app.platform.storage.load(
            [self.FOLDER], key
        )


class GenericRoutingHookImpl(HookBase):
    """Can only be used to make sure that a hook has been dispatched or not"""

    def __init__(self):
        super().__init__()

    def __call__(self, routing_table: "RoutingTable", *args, **kwargs) -> None:
        assert kwargs.get("dimensions", None) is not None, "Output 'dimensions' map not in hook params"
        assert kwargs.get("dimensions_map", None) is not None, "Output 'dimensions_map' map not in hook params"
        super()._save(routing_table)


class OnExecBeginHookImpl(HookBase, IExecutionBeginHook):
    """A specific hook impl designed to pass as 'RouteExecutionHook::on_exec_begin'"""

    # TODO input parameters for other checks (at route_record, execution_context level)
    def __init__(self):
        super().__init__()

    def __call__(
        self,
        routing_table: "RoutingTable",
        route_record: "RoutingTable.RouteRecord",
        execution_context: "Route.ExecutionContext",
        current_timestamp_in_utc: int,
        **params,
    ) -> None:
        super()._save(routing_table)
        # TODO other checks


class GenericComputeDescriptorHookVerifier(ComputeDescriptor, GenericRoutingHookImpl):
    """Designed to be used in tests to make sure that aggregated compute descriptor instance (e.g Email or custom)
    is compatible with the hook callbacks that the instances of this class will be assigned to.

    Example:

        email_obj = EMAIL(sender="if-test-list@amazon.com",
                          recipient_list=["yunusko@amazon.com"])
        on_exec_hook_begin_wrapper = GenericComputeDescriptorHookVerifier(email_obj)
        on_compute_failed_wrapper= GenericComputeDescriptorHookVerifier(email_obj)

        new_data = app.create_data(execution_hook=RouteExecutionHook(on_exec_begin=exec_hook_begin_wrapper, on_compute_failed=on_compute_failed_wrapper)

        # assume that this will succeed in the test
        app.execute(new_data)

        assert on_exec_hook_begin_wrapper.verify()
        assert not on_compute_failed_wrapper.verify()
    """

    def __init__(self, builtin_compute: ComputeDescriptor):
        GenericRoutingHookImpl.__init__(self)
        self._builtin_compute = builtin_compute

    # overrides (ComputeDescriptor::parametrize)
    def parametrize(self, platform: "HostPlatform") -> None:
        self._builtin_compute.parametrize(platform)

    def activation_completed(self, platform: "HostPlatform") -> None:
        self._builtin_compute.activation_completed(platform)

    # overrides (ComputeDescriptor::create_slot)
    def create_slot(self, output_signal: Signal) -> Slot:
        org_slot = self._builtin_compute.create_slot(output_signal)
        # hijack org_slot.code and register this wrapper to be able to hit __call__ during tests.
        return Slot(
            org_slot.type,
            dumps(self),
            org_slot.code_lang,
            org_slot.code_abi,
            org_slot.extra_params,
            org_slot.output_signal_desc,
            org_slot.permissions,
            org_slot.retry_count,
        )

    def __call__(self, routing_table: "RoutingTable", *args, **kwargs) -> None:
        try:
            self._builtin_compute(routing_table, *args, **kwargs)
        except ComputeInternalError:
            # this IF exception means that the builtin operation was in control. that is what we care about here, to
            # catch compatibility issues.
            pass

        # builtin compute did not freak out, mark it as successful dispatch
        GenericRoutingHookImpl.__call__(self, routing_table, *args, **kwargs)
