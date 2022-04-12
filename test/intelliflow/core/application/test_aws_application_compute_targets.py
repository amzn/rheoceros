# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from mock import MagicMock

from intelliflow.api_ext import *
from intelliflow.core.platform import development as development_module
from intelliflow.core.platform.compute_targets.email import EMAIL
from intelliflow.core.platform.constructs import ConstructPermission
from intelliflow.core.platform.definitions.compute import (
    ComputeResponse,
    ComputeSessionDesc,
    ComputeSessionState,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.signal_processing import Slot
from intelliflow.core.signal_processing.signal import *
from intelliflow.core.signal_processing.slot import SlotType
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.inlined_compute import InlinedComputeRetryVerifier


class TestAWSApplicationComputeTargets(AWSTestBase):
    class CustomHybridCompute(ComputeDescriptor, IExecutionBeginHook):
        def __init__(self):
            self.compute_parametrize_called = False
            self.activation_completed_called = False

        # overrides
        def parametrize(self, platform: "HostPlatform") -> None:
            """System calls this to allow descriptor impl to align parameters on the new Slot object to be returned
            from within create_slot."""
            assert not self.compute_parametrize_called
            self.compute_parametrize_called = True

            assert isinstance(platform, HostPlatform)

        # overrides
        def create_slot(self, output_signal: Signal) -> Slot:
            # make sure that parametrization happens before the Slot is instantiated and returned back to the sys.
            assert self.compute_parametrize_called
            return Slot(
                type=SlotType.SYNC_INLINED,
                code=dumps(lambda: print("Hello World")),
                code_lang=None,
                code_abi=None,
                extra_params=dict(),
                output_signal_desc=output_signal,
                compute_permissions=None,
                retry_count=0,
            )

        # overides
        def activation_completed(self, platform: "HostPlatform") -> None:
            self.activation_completed_called = True

        # overrides (IExecutionBeginHook)
        def __call__(
            self,
            routing_table: "RoutingTable",
            route_record: "RoutingTable.RouteRecord",
            execution_context: "Route.ExecutionContext",
            current_timestamp_in_utc: int,
            **params
        ) -> None:
            pass

    def test_application_custom_compute_descriptor(self):
        """Test the common ComputeDescriptor interface which is used by system provided compute targets such as Email,
        and also inherited by front-end descriptors like InternalDataNode::InlinedComputeDescriptor"""
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app_name: str = "compute-targets"
        self.app = AWSApplication(app_name, self.region)

        CustomCompute = TestAWSApplicationComputeTargets.CustomHybridCompute

        custom_compute = CustomCompute()
        self.app.create_data(id="dummy_node", compute_targets=[custom_compute])

        assert custom_compute.compute_parametrize_called
        assert not custom_compute.activation_completed_called

        self.app.activate()

        assert custom_compute.activation_completed_called

        # now test the effect of update_data on ComputeDescriptors. Descriptors from most recently updated nodes will be
        # notified during activation, not an older version of the same node (created by create_data or updated by an
        # earlier update_data call).

        custom_compute_1 = CustomCompute()
        custom_compute_hook_1 = CustomCompute()
        self.app.create_data(
            id="order_test_node", compute_targets=[custom_compute_1], execution_hook=RouteExecutionHook(on_exec_begin=custom_compute_hook_1)
        )
        # update the same node by changing the hook
        custom_compute_2 = CustomCompute()
        custom_compute_hook_2 = CustomCompute()
        self.app.update_data(
            id="order_test_node", compute_targets=[custom_compute_2], execution_hook=RouteExecutionHook(on_exec_begin=custom_compute_hook_2)
        )

        # before the activation all should be false
        assert not custom_compute_1.activation_completed_called
        assert not custom_compute_2.activation_completed_called
        assert not custom_compute_hook_1.activation_completed_called
        assert not custom_compute_hook_2.activation_completed_called

        self.app.activate()

        # after the activation descriptors from the earlier version of the node (from create_data call) are not
        # notified since they are overwritten by a new versions
        assert not custom_compute_1.activation_completed_called
        assert custom_compute_2.activation_completed_called
        assert not custom_compute_hook_1.activation_completed_called
        assert custom_compute_hook_2.activation_completed_called

        self.patch_aws_stop()

    def test_application_system_compute_descriptors(self):
        """Test system provided compute targets' compatibility"""
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        self.app = AWSApplication("sys-targets", self.region)

        email_obj = EMAIL(sender="if-test-list@amazon.com", recipient_list=["yunusko@amazon.com"])

        self.app.create_data(id="dummy_node", compute_targets=[email_obj.action()])

        # Test permissions applied to runtime / exec role as well
        # keep reference of actual policy updater method so that we can retore it at the end.
        real_put_inlined_policy = development_module.put_inlined_policy

        def put_inlined_policy(
            role_name: str, policy_name: str, action_resource_pairs: Set[ConstructPermission], base_session: "boto3.Session"
        ) -> None:
            if "IntelliFlowExeRole" in role_name:
                # check EMAIL resource in runtime permission resources (SES ARN, etc)
                assert any([email_obj.sender in resource for perm in action_resource_pairs for resource in perm.resource])

        development_module.put_inlined_policy = MagicMock(side_effect=put_inlined_policy)

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = self.app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        self.app._dev_context = dev_context
        #

        # above mock / callback should be called during the activation
        self.app.activate()
        # just make sure that it was called actually (otherwise there is no point in this test :)
        assert development_module.put_inlined_policy.call_count > 0

        # restore
        development_module.put_inlined_policy = real_put_inlined_policy

        self.patch_aws_stop()

    def test_application_inlined_compute_implicit_retry(self):
        """Test how IF orchestrates inlined compute executions that utilize specific compute exception types provided
        by the framework itself:
            - ComputeRetryableInternalError
            - ComputeInternalError

        IF is expected to revisit inlined computes that keep throwing ComputeRetryableInternalError and terminate the
        execution (or more specifically the compute since an execution can have multiple computes) in other errors.
        """
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app_name: str = "implicit-retries"
        app = AWSApplication(app_name, self.region)

        # TODO storage._bucket_name is a hack till IInlinedCompute interface change.
        inlined_compute_that_retries = InlinedComputeRetryVerifier(retry_count=2, storage_bucket=app.platform.storage._bucket_name)

        retry_node = app.create_data(id="dummy_node", compute_targets=[InlinedCompute(inlined_compute_that_retries)])

        app.execute(retry_node, wait=False)

        # first attemp has failed, did not get retried yet, so should fail
        assert not inlined_compute_that_retries.verify(app)

        # in unit-test, remote Processor won't work, System clock won't trigger the signals for periodical cycles in the
        # processor that would revisit the routes and retries on pending inlined computes.
        # so in unit-tests, we can cause 'next cycle' in by doing the following.
        app.update_active_routes_status()

        # must have been retried once, but should still fail since verifier expects two retries
        assert not inlined_compute_that_retries.verify(app)

        app.update_active_routes_status()

        # should succeed!
        assert inlined_compute_that_retries.verify(app)

        # now do another (redundant) cycle to make sure that IF moves fully retried executions to inactive records and won't
        # mistakenly retry again.
        app.update_active_routes_status()
        # should still succeed!
        assert inlined_compute_that_retries.verify(app)

        #  if compute was called again in the redundant cycle, the verifier would have failed the execution after
        # detecting the condition of "current_retry_count > (intended) retry_count"
        # check successful completion/execution also
        path, _ = app.poll(retry_node)
        assert path

        self.patch_aws_stop()

    def test_application_batch_compute_parametrization(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("slot-params", self.region)

        kickoff_node = app.create_data(
            id="ADEX_BladeKickOffJob",
            compute_targets=[
                BatchCompute(
                    scala_script(
                        """ this won't get executed in this unit-test, so don't matter. """,
                        external_library_paths=["s3://if-awsglue-adex-ml-230392972774-us-east-1/batch/DexmlBladeGlue-super.jar"],
                    ),
                    lang=Lang.SCALA,
                    GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=2,
                    Timeout=3 * 60,  # 3 hours
                    my_param="PARAM1",
                    args1="v1",
                )
            ],
            dataset_format=DatasetSignalSourceFormat.PARQUET,
        )

        app.activate()

        # BEGIN mock BatchCompute to yield successful execution
        # mock batch_compute for instant success on batch jobs
        def compute(
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

            return TestAWSApplicationExecutionControl.create_batch_compute_response(ComputeSuccessfulResponseType.COMPLETED, "job_1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        # END mock

        # 1- now execute and check parametrization has been done successfully, directly using the route object
        #  that represents the output (from the RoutingTable). If we were only interested in checking this, then we
        # could have used the signal of the return value of the create_data call. However, we are also interested in how
        # IF manages the changeset (integrity change) of a route during activation. This is important because we had a
        # bug where external_library_paths were not detected as changed and the state in remote RoutingTable was not
        # invalidated from development version. Note: IF does not invalidate/update an route to keep existing pending
        # nodes and active computes still tracked by the orchestration. In all of the other cases (integrity, semantical
        # change), IF invalidates the route. It is strict about it.

        # this will both activate and then execute the node ending up with an active route in the RoutingTable.
        app.execute(kickoff_node)

        kickoff_route = app.get_active_route(kickoff_node).route
        assert isinstance(kickoff_route.slots[0].code, SlotCode)
        assert len(cast(SlotCode, kickoff_route.slots[0].code).metadata.external_library_paths) == 1

        assert len(kickoff_route.slots[0].extra_params.keys()) == 6
        assert set(kickoff_route.slots[0].extra_params.keys()) == {
            "GlueVersion",
            "WorkerType",
            "NumberOfWorkers",
            "Timeout",
            "my_param",
            "args1",
        }

        # now emulate another development session where kick_off node will be updated
        app = AWSApplication("slot-params", self.region)
        app.attach()

        # 2- Now prove that external_library_paths will get reflected in the RoutingTable
        #    keep everything else same.
        kickoff_node = app.update_data(
            id="ADEX_BladeKickOffJob",
            compute_targets=[
                BatchCompute(
                    scala_script(
                        """ this won't get executed in this unit-test, so don't matter. """,
                        external_library_paths=[
                            "s3://if-awsglue-adex-ml-230392972774-us-east-1/batch/DexmlBladeGlue-super.jar",
                            "s3://THIS_IS/A NEW LIBRARY PATH",
                        ],
                    ),
                    lang=Lang.SCALA,
                    GlueVersion="2.0",
                    WorkerType=GlueWorkerType.G_1X.value,
                    NumberOfWorkers=2,
                    Timeout=3 * 60,  # 3 hours
                    my_param="PARAM1",
                    args1="v1",
                )
            ],
            dataset_format=DatasetSignalSourceFormat.PARQUET,
        )

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        # during this activation RoutingTable should invalidate the active route record
        app.activate()
        assert not app.get_active_route(kickoff_node)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)

        app.execute(kickoff_node)  # will insert the new version of the route into the RoutingTable

        kickoff_route = app.get_active_route(kickoff_node).route
        # now it should give 2!
        assert len(cast(SlotCode, kickoff_route.slots[0].code).metadata.external_library_paths) == 2

        self.patch_aws_stop()

    def test_application_batch_compute_default_parametrization_GLUE(self):
        self.patch_aws_start(glue_catalog_has_all_tables=True)

        app = AWSApplication("slot-params", self.region)

        kickoff_node = app.create_data(
            id="ADEX_BladeKickOffJob",
            compute_targets=[
                Glue(
                    "SPARK CODE HERE",
                )
            ],
        )

        kickoff_node_2 = app.create_data(id="ADEX_BladeKickOffJob_2", compute_targets=[Glue("SPARK CODE HERE", WorkerType="G.2X")])

        kickoff_node_3 = app.create_data(id="ADEX_BladeKickOffJob_3", compute_targets=[Glue("SPARK CODE HERE", NumberOfWorkers=100)])

        node_2 = app.create_data(
            id="NODE_2", compute_targets=[Glue("SPARK CODE 2", GlueVersion=GlueVersion.VERSION_1_0.value, MaxCapacity=50)]
        )

        app.activate()

        # BEGIN mock BatchCompute to yield successful execution
        # mock batch_compute for instant success on batch jobs
        def compute(
            materialized_inputs: List[Signal],
            slot: Slot,
            materialized_output: Signal,
            execution_ctx_id: str,
            retry_session_desc: Optional[ComputeSessionDesc] = None,
        ) -> ComputeResponse:
            from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

            return TestAWSApplicationExecutionControl.create_batch_compute_response(ComputeSuccessfulResponseType.COMPLETED, "job_1")

        def get_session_state(session_desc: ComputeSessionDesc, active_compute_record: "RoutingTable.ComputeRecord") -> ComputeSessionState:
            from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

            return TestAWSApplicationExecutionControl.create_batch_compute_session_state(ComputeSessionStateType.COMPLETED)

        app.platform.batch_compute.compute = MagicMock(side_effect=compute)
        app.platform.batch_compute.get_session_state = MagicMock(side_effect=get_session_state)
        # END mock

        # 1- now execute and check parametrization has been done successfully, directly using the route object
        #  that represents the output (from the RoutingTable). If we were only interested in checking this, then we
        # could have used the signal of the return value of the create_data call. However, we are also interested in how
        # IF manages the changeset (integrity change) of a route during activation. This is important because we had a
        # bug where external_library_paths were not detected as changed and the state in remote RoutingTable was not
        # invalidated from development version. Note: IF does not invalidate/update an route to keep existing pending
        # nodes and active computes still tracked by the orchestration. In all of the other cases (integrity, semantical
        # change), IF invalidates the route. It is strict about it.

        # this will both activate and then execute the node ending up with an active route in the RoutingTable.
        app.execute(kickoff_node)
        app.execute(kickoff_node_2)
        app.execute(kickoff_node_3)
        app.execute(node_2)

        kickoff_route = app.get_active_route(kickoff_node).route
        assert len(kickoff_route.slots[0].extra_params.keys()) == 4
        assert set(kickoff_route.slots[0].extra_params.keys()) == {"GlueVersion", "WorkerType", "NumberOfWorkers", "Timeout"}

        kickoff_route = app.get_active_route(kickoff_node_2).route
        assert set(kickoff_route.slots[0].extra_params.keys()) == {"GlueVersion", "WorkerType", "NumberOfWorkers", "Timeout"}

        kickoff_route = app.get_active_route(kickoff_node_3).route
        assert set(kickoff_route.slots[0].extra_params.keys()) == {"GlueVersion", "WorkerType", "NumberOfWorkers", "Timeout"}

        route = app.get_active_route(node_2).route
        assert len(route.slots[0].extra_params.keys()) == 3
        assert set(route.slots[0].extra_params.keys()) == {"GlueVersion", "MaxCapacity", "Timeout"}

        # create a new dev context
        app = AWSApplication("slot-params", self.region)

        app.create_data(
            id="SHOULD_FAIL",
            compute_targets=[
                Glue(
                    "SPARK CODE HERE",
                    # cannot be defined at the same time
                    MaxCapacity=50,
                    WorkerType="G.1X",
                )
            ],
        )

        with pytest.raises(ValueError):
            app.activate()

        app = AWSApplication("slot-params", self.region)

        app.create_data(
            id="SHOULD_FAIL",
            compute_targets=[
                Glue(
                    "SPARK CODE HERE",
                    # cannot be defined at the same time
                    MaxCapacity=50,
                    NumberOfWorkers=100,
                )
            ],
        )

        with pytest.raises(ValueError):
            app.activate()

        app = AWSApplication("slot-params", self.region)

        node_3 = app.create_data(
            id="NODE_3",
            compute_targets=[
                Glue(
                    "SPARK CODE HERE",
                    # cannot be used in GlueVersion > 1.0
                    # in auto mode, currently we are not using 1.0
                    MaxCapacity=50,
                )
            ],
        )

        with pytest.raises(ValueError):
            app.activate()

        self.patch_aws_stop()
