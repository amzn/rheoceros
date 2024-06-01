# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import copy
from typing import cast
import sys
from unittest.mock import MagicMock

import pytest

from intelliflow.api import DataFormat, S3Dataset
from intelliflow.api_ext import HADOOP_SUCCESS_FILE, AWSApplication
from intelliflow.core.application.application import Application
from intelliflow.core.application.context.node.internal.nodes import InternalDataNode
from intelliflow.core.application.core_application import ApplicationState
from intelliflow.core.permission import Permission, PermissionContext
from intelliflow.core.platform.constructs import RoutingTable
from intelliflow.core.platform.definitions.common import ActivationParams
from intelliflow.core.platform.definitions.compute import (
    ComputeResourceDesc,
    ComputeSessionDesc,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.platform.development import AWSConfiguration, HostPlatform
from intelliflow.core.platform.drivers.processor.aws import AWSLambdaProcessorBasic
from intelliflow.core.signal_processing.definitions.dimension_defs import Type
from intelliflow.core.signal_processing.signal import *
from intelliflow.mixins.aws.test import AWSTestBase
from intelliflow.utils.test.inlined_compute import NOOPCompute


class TestAWSApplication(AWSTestBase):
    def _create_test_application(self, id: str, enforce_runtime_compatibility: bool = True):
        return Application(
            id,
            HostPlatform(AWSConfiguration.builder().with_default_credentials().with_region(self.region).build()),
            enforce_runtime_compatibility,
        )

    def test_application_init(self):
        self.patch_aws_start()
        app = self._create_test_application("test_app")
        assert app
        assert app.uuid
        assert app.platform

        app2 = self._create_test_application("test_app2")
        assert app2
        assert app2.uuid
        assert app2.platform

        assert app.id != app2.id
        assert app.uuid != app2.uuid

        app3 = Application(
            "test_app",
            HostPlatform(AWSConfiguration.builder().with_default_credentials().with_region("us-west-2").build()),  # change the region
        )

        assert app.id == app3.id
        assert app.uuid != app3.uuid

        self.patch_aws_stop()

    def test_application_init_params(self):
        """Test high-level configuration for AWS drivers"""
        self.patch_aws_start()

        app = Application(
            "test_params",
            HostPlatform(
                AWSConfiguration.builder()
                .with_default_credentials()
                .with_region("us-west-2")
                .with_param(AWSLambdaProcessorBasic.CORE_LAMBDA_CONCURRENCY_PARAM, 5)
                .build()
            ),  # change the region
        )

        assert app.platform.processor.desired_core_lambda_concurrency == 5
        self.patch_aws_stop()

    def test_application_init_params_for_permissions(self):
        """Test high-level configuration for AWS Configuration"""
        self.patch_aws_start()

        app = Application(
            "test_perms",
            HostPlatform(
                AWSConfiguration.builder()
                .with_default_credentials()
                .with_region("us-west-2")
                .with_permissions(
                    [
                        Permission(resource=["BOTH"], action=["BOTH_ACTION"]),
                        Permission(resource=["EXEC"], action=["EXEC_ACTION"], context=PermissionContext.RUNTIME),
                        Permission(resource=["DEV"], action=["DEV_ACTION"], context=PermissionContext.DEVTIME),
                    ]
                )
                .with_default_policies({"AWSS3ReadOnlyAccess"})
                .with_trusted_entities(["AN_ENTITY_ARN"])
                .build()
            ),
        )
        assert app.platform.conf.provide_runtime_default_policies().issuperset({"AWSS3ReadOnlyAccess"})
        assert "AN_ENTITY_ARN" in app.platform.conf.provide_runtime_trusted_entities()
        # check runtime perms (that will go into exec role)
        assert any(
            const_perm.resource == ["BOTH"] and const_perm.action == ["BOTH_ACTION"]
            for const_perm in app.platform.conf.provide_runtime_construct_permissions()
        )
        assert any(
            const_perm.resource == ["EXEC"] and const_perm.action == ["EXEC_ACTION"]
            for const_perm in app.platform.conf.provide_runtime_construct_permissions()
        )
        assert not any(const_perm.resource == ["DEV"] for const_perm in app.platform.conf.provide_runtime_construct_permissions())
        # check devtime perms (that will go into dev role)
        assert any(
            const_perm.resource == ["BOTH"] and const_perm.action == ["BOTH_ACTION"]
            for const_perm in app.platform.conf.provide_devtime_construct_permissions()
        )
        assert any(
            const_perm.resource == ["DEV"] and const_perm.action == ["DEV_ACTION"]
            for const_perm in app.platform.conf.provide_devtime_construct_permissions()
        )
        assert not any(const_perm.resource == ["EXEC"] for const_perm in app.platform.conf.provide_devtime_construct_permissions())

        self.patch_aws_stop()

    def test_application_terminate_without_activation(self):
        self.patch_aws_start()

        app = self._create_test_application("test_app")
        # fail due to state error (should be active or paused)
        with pytest.raises(Exception):
            app.terminate()

        app.delete()

        self.patch_aws_stop()

    def test_application_activate(self):
        self.patch_aws_start()
        app = self._create_test_application("test_app")
        # flag intentionally set to False to capture the code path for an INACTIVE application concurrency
        # check not causing any issues (skipped actually).
        app.activate(allow_concurrent_executions=False)

        # load again
        app2 = self._create_test_application("test_app")
        assert app2.platform
        assert app.uuid == app2.uuid

        app.update_active_routes_status = MagicMock()

        app2.activate(allow_concurrent_executions=False)
        assert app.update_active_routes_status.call_count == 0

        assert app2.state == ApplicationState.ACTIVE

        self.patch_aws_stop()

    def test_application_terminate_after_activation(self):
        self.patch_aws_start()
        app = self._create_test_application("test_app")
        app.activate()

        with pytest.raises(Exception):
            app.delete()

        app.terminate()

        with pytest.raises(Exception):
            app.terminate()

        assert app.state == ApplicationState.INACTIVE

        app.delete()

        assert app.state == ApplicationState.DELETED

        self.patch_aws_stop()

    def test_application_termination_resilience(self):
        self.patch_aws_start()
        app = self._create_test_application("test_app")
        external_data = app.marshal_external_data(
            S3Dataset("111222333444", "bucke", "prefix", "{}", dataset_format=DataFormat.CSV),
            "test_ext_data",
            {"region": {"type": Type.STRING}},
            {
                "NA": {},
            },
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        app.create_data(
            id="test_internal_data",
            inputs=[external_data],
            input_dim_links=[],
            output_dimension_spec={},
            output_dim_links={},
            compute_targets=[InternalDataNode.InlinedComputeDescriptor(lambda input_map, output, params: print("oh hello"))],
        )

        app.activate()

        # make termination fail half-way through (check resilience in against different modules)
        # 1- processor fails and then termination is resumed manually
        original_terminate = app.platform.processor.terminate
        app.platform.processor.terminate = MagicMock(side_effect=RuntimeError())
        try:
            app.terminate()
        except RuntimeError:
            pass
        assert app.state == ApplicationState.TERMINATING
        app.platform.processor.terminate = original_terminate
        app.terminate()
        assert app.state == ApplicationState.INACTIVE

        # 2- processor_queue
        app.activate()
        original_terminate = app.platform.processor_queue.terminate
        app.platform.processor_queue.terminate = MagicMock(side_effect=RuntimeError())
        try:
            app.terminate()
        except RuntimeError:
            pass
        assert app.state == ApplicationState.TERMINATING
        app.platform.processor_queue.terminate = original_terminate
        app.terminate()
        assert app.state == ApplicationState.INACTIVE

        # 3- batch_compute
        app.activate()
        original_terminate = app.platform.batch_compute.terminate
        app.platform.batch_compute.terminate = MagicMock(side_effect=RuntimeError())
        try:
            app.terminate()
        except RuntimeError:
            pass
        assert app.state == ApplicationState.TERMINATING
        app.platform.batch_compute.terminate = original_terminate
        app.terminate()
        assert app.state == ApplicationState.INACTIVE

        # 4- routing_table
        app.activate()
        original_terminate = app.platform.routing_table.terminate
        app.platform.routing_table.terminate = MagicMock(side_effect=RuntimeError())
        try:
            app.terminate()
        except RuntimeError:
            pass
        assert app.state == ApplicationState.TERMINATING
        app.platform.routing_table.terminate = original_terminate
        app.terminate()
        assert app.state == ApplicationState.INACTIVE

        # and finally test that a TERMINATING application can be restored back to active state
        app.activate()
        original_terminate = app.platform.processor.terminate
        app.platform.processor.terminate = MagicMock(side_effect=RuntimeError())
        try:
            app.terminate()
        except RuntimeError:
            pass
        app.platform.processor.terminate = original_terminate
        app.activate()
        assert app.state == ApplicationState.ACTIVE

        self.patch_aws_stop()

    def test_application_activate_concurrency(self):
        from test.intelliflow.core.signal_processing.routing_runtime_constructs.test_route import TestRoute
        from test.intelliflow.core.signal_processing.signal.test_signal import TestSignal

        self.patch_aws_start()

        app = self._create_test_application("test_app")
        # needed to be able to insert an active record below (simulate real-life scenario)
        app.activate()

        # setup an active record
        compute_resp = ComputeSuccessfulResponse(
            ComputeSuccessfulResponseType.PROCESSING, ComputeSessionDesc("job_id", ComputeResourceDesc("job_name", "job_arn"))
        )
        test_route = TestRoute._route_1_basic()

        test_route_record = RoutingTable.RouteRecord(test_route.clone())
        compute_record = RoutingTable.ComputeRecord(
            1608154549,
            [TestSignal.signal_internal_1],
            test_route.slots[0],
            test_route.output,
            "8fd9995c-af42-4cf2-a2ca-e62eacce44f0",
            deactivated_timestamp_utc=1608154579,
            state=compute_resp,
        )
        test_route_record.add_active_compute_record(compute_record)
        app.platform.routing_table._save(test_route_record)
        # end setup

        def complete_active_record():
            app.platform.routing_table._delete({test_route})

        app.platform.routing_table.check_active_routes = MagicMock(side_effect=complete_active_record)
        app.platform.processor.pause = MagicMock()
        app.platform.processor.resume = MagicMock()

        # now try to activate
        app._save = MagicMock()  # unnecessary for this test and might cause PicklingError due to mocks.
        app.platform.activate = MagicMock()  # unnecessary for this test and might cause PicklingError due to mocks.
        app.activate(allow_concurrent_executions=False)
        assert app.platform.processor.pause.call_count == 1
        assert app.platform.routing_table.check_active_routes.call_count == 1
        assert app.platform.activate.call_count == 1
        assert app.platform.processor.resume.call_count == 1

        self.patch_aws_stop()

    def test_application_context_management(self):
        self.patch_aws_start()
        app = self._create_test_application("test_app")
        external_data = app.marshal_external_data(
            S3Dataset("111222333444", "bucke", "prefix", "{}", dataset_format=DataFormat.CSV),
            "test_ext_data",
            {"region": {"type": Type.STRING}},
            {
                "NA": {},
            },
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        def example_inline_compute_code(input_map, output, params):
            """Function to be called within RheocerOS core.
            Below parameters and also inputs as local variables in both
                - alias form ('order_completed_count_amt')
                - indexed form  (input0, input1, ...)
            will be available within the context.

            input_map: [alias -> input signal]
            output: output signal
            params: contains platform parameters (such as 'AWS_BOTO_SESSION' if an AWS based configuration is being used)

            Ex:
            s3 = params['AWS_BOTO_SESSION'].resource('s3')
            """
            print("Hello from AWS!")

        app.create_data(
            id="test_internal_data",
            inputs=[external_data],
            input_dim_links=[],
            output_dimension_spec={},
            output_dim_links={},
            compute_targets=[InternalDataNode.InlinedComputeDescriptor(example_inline_compute_code)],
        )
        app.save_dev_state()

        # reload
        app = self._create_test_application("test_app")

        assert not app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert not app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert not app.get_data(
            "test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT
        )
        assert not app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app.load_dev_state()
        assert not app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert not app.get_data(
            "test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT
        )
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app.activate()
        assert app.state == ApplicationState.ACTIVE
        assert app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app = self._create_test_application("test_app")
        assert app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert not app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert not app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app.attach()
        assert app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app.terminate()
        assert not app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert not app.get_data(
            "test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT
        )
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        assert app.state == ApplicationState.INACTIVE

        app.activate()
        assert app.state == ApplicationState.ACTIVE
        assert app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app.terminate()
        # as tested above, now active context is empty but we still have dev context intact. However, the following
        # reload will invalidate dev context.
        app = self._create_test_application("test_app")
        assert app.state == ApplicationState.INACTIVE
        # as explained above, both active and dev contexts are gone at this point.
        assert not app.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ALL)

        self.patch_aws_stop()

    def test_application_refresh(self):
        self.patch_aws_start()
        app = self._create_test_application("test_app")
        app_2 = self._create_test_application("test_app")

        external_data = app.marshal_external_data(
            S3Dataset("111222333444", "bucke", "prefix", "{}", dataset_format=DataFormat.CSV),
            "test_ext_data",
            {"region": {"type": Type.STRING}},
            {
                "NA": {},
            },
            SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
        )

        app.create_data(
            id="test_internal_data",
            inputs=[external_data],
            input_dim_links=[],
            output_dimension_spec={},
            output_dim_links={},
            compute_targets=[NOOPCompute],
        )
        app.activate()

        # let's assume that app_2 instance is used in a different development environment.
        # without a refresh, it won't able to see the changes that have just been deployed via activation above.
        assert not app_2.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert not app_2.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert not app_2.get_data(
            "test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT
        )
        assert not app_2.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app_2.refresh()
        assert app_2.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert not app_2.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert app_2.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert not app_2.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        app_2.attach()
        assert app_2.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app_2.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)
        assert app_2.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app_2.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.DEV_CONTEXT)

        # but platform is not sync. driver instanes still don't know about the activation (new state)
        assert len(app_2.platform.routing_table._route_index.get_all_routes()) == 0

        app_2.refresh(full_stack=True)

        # active context must have pulled in
        assert app_2.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert app_2.get_data("test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)

        # and routing index as well
        assert len(app_2.platform.routing_table._route_index.get_all_routes()) == 1

        # reset the other application
        app = self._create_test_application("test_app")
        app.activate()

        # do a full stack refresh again
        app_2.refresh(full_stack=True)
        # everything should be wiped out
        assert not app_2.get_data("test_ext_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT)
        assert not app_2.get_data(
            "test_internal_data", Application.QueryApplicationScope.ALL, Application.QueryContext.ACTIVE_RUNTIME_CONTEXT
        )
        # and routing index as well
        assert len(app_2.platform.routing_table._route_index.get_all_routes()) == 0

        self.patch_aws_stop()

    def test_application_pause_resume(self):
        self.patch_aws_start()

        app = self._create_test_application("test_app")

        app.platform.processor.pause = MagicMock()
        app.platform.processor.resume = MagicMock()
        # no-op
        app.pause()
        assert app.state == ApplicationState.INACTIVE
        assert app.platform.processor.pause.call_count == 0

        # no-op
        app.resume()
        assert app.state == ApplicationState.INACTIVE
        assert app.platform.processor.resume.call_count == 0

        app = self._create_test_application("test_app")
        app.activate()
        app.platform.processor.pause = MagicMock()
        assert app.state == ApplicationState.ACTIVE
        app.pause()
        assert app.state == ApplicationState.PAUSED
        assert app.platform.processor.pause.call_count == 1

        # reload
        app = self._create_test_application("test_app")
        assert app.state == ApplicationState.PAUSED

        app.platform.processor.resume = MagicMock()
        app.resume()
        assert app.state == ApplicationState.ACTIVE
        assert app.platform.processor.resume.call_count == 1

        self.patch_aws_stop()

    def test_application_common_pitfalls_external_data(self):
        self.patch_aws_start()
        app = self._create_test_application("test_pitfalls")

        with pytest.raises(ValueError):
            # fail due to missing ID as the second positional argument and spec being used as ID mistakenly.
            app.marshal_external_data(
                S3Dataset("111222333444", "bucke", "prefix", "{}", dataset_format=DataFormat.CSV),
                {"region": {"type": Type.STRING}},
                {
                    "NA": {},
                },
                SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
            )

        with pytest.raises(ValueError):
            # fail due to 'protocol' being used as the 3rd positional arg 'dimension_filter'
            app.marshal_external_data(
                S3Dataset("111222333444", "bucke", "prefix", "{}", dataset_format=DataFormat.CSV),
                "test_ext_data",
                {"region": {"type": Type.STRING}},
                SignalIntegrityProtocol("FILE_CHECK", {"file": "_SUCCESS"}),
            )

        with pytest.raises(ValueError):
            # fail due to 'protocol' being wrong type
            app.marshal_external_data(
                S3Dataset("111222333444", "bucke", "prefix", "{}", dataset_format=DataFormat.CSV),
                "test_ext_data",
                {"region": {"type": Type.STRING}},
                {
                    "NA": {},
                },
                "PROTOCOL",
            )

        app.marshal_external_data(
            S3Dataset("111222333444", "bucke", "prefix", "{}", dataset_format=DataFormat.CSV),
            "external_data_ID",
            {"region": {"type": Type.STRING}},
            {
                "NA": {},
            },
            HADOOP_SUCCESS_FILE,
        )

        self.patch_aws_stop()

    def test_application_incompatible_runtime(self):
        self.patch_aws_start()

        app = AWSApplication("test_app_rt_err", self.region)

        # 1- setup temporary node module (with InlinedCompute and dim link mapper functions)
        import importlib
        from importlib.resources import path

        test_resources_spec = importlib.util.find_spec("test.intelliflow.core.application.test_resources")
        root_mod = importlib.util.module_from_spec(test_resources_spec)

        file_path = None
        with path(root_mod, "test_node.py") as resource_path:
            file_path = resource_path

        tmp_file_path = file_path.parent / "test_node_tmp.py"
        if tmp_file_path.exists():
            tmp_file_path.unlink()
        tmp_file_path.symlink_to(file_path)
        from test.intelliflow.core.application.test_resources import test_node_tmp

        # 2- use the new module
        test_node_tmp.join(app)

        # serializes app and platform state implicitly
        app.activate()

        # 3- test successfuly sync while the module is still there
        app = AWSApplication("test_app_rt_err", self.region, enforce_runtime_compatibility=True)

        # 4- emulate "incompatible" change in code base by removing the node module
        del test_node_tmp
        tmp_file_path.unlink()
        del sys.modules["test.intelliflow.core.application.test_resources.test_node_tmp"]

        # fail due to state error (should be active or paused)
        with pytest.raises(Application.IncompatibleRuntime):
            app = AWSApplication("test_app_rt_err", self.region, enforce_runtime_compatibility=True)

        # test successful reset of active state when compatibility is not enforced
        app = AWSApplication("test_app_rt_err", self.region, enforce_runtime_compatibility=False)

        assert app.state == ApplicationState.INACTIVE
        # use a method that would do a traversal on the platform and underlying drivers post-reset.
        # e.g if a driver has not been instantiated successfully (or the app is in terminated state, then it will fail).
        assert app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)

        self.patch_aws_stop()

    def test_application_remote_dev_env_support(self):
        self.patch_aws_start()

        app = AWSApplication("test_app_rde", self.region, **{ActivationParams.REMOTE_DEV_ENV_SUPPORT.value: False})

        with pytest.raises(ValueError):
            app.provision_remote_dev_env(dict())

        app = Application(
            "test_app_rde2",
            HostPlatform(
                AWSConfiguration.builder().with_default_credentials().with_remote_dev_env_support(False).with_region(self.region).build()
            ),
        )

        with pytest.raises(ValueError):
            app.provision_remote_dev_env(dict())

        self.patch_aws_stop()
