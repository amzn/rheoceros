# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import os
import threading
import time
from typing import Dict, Optional, Union
from unittest.mock import MagicMock

import boto3
import mock
import pytest
from moto import (
    mock_applicationautoscaling,
    mock_athena,
    mock_autoscaling,
    mock_cloudwatch,
    mock_dynamodb2,
    mock_events,
    mock_glue,
    mock_iam,
    mock_kms,
    mock_lambda,
    mock_s3,
    mock_ses,
    mock_sns,
    mock_sqs,
    mock_sts,
)

import intelliflow.core.application.context.node.external.nodes as external_nodes
import intelliflow.core.platform.drivers.compute.aws as compute_driver
import intelliflow.core.platform.drivers.compute.aws_athena as athena_compute_driver
import intelliflow.core.platform.drivers.compute.aws_emr as emr_compute_driver
import intelliflow.core.platform.drivers.diagnostics.aws as diagnostics_driver
import intelliflow.core.platform.drivers.processor.aws as processor_driver
import intelliflow.core.platform.drivers.routing.aws as routing_driver
import intelliflow.core.platform.drivers.storage.aws as storage_driver
import intelliflow.core.signal_processing.routing_runtime_constructs as routing_runtime_constructs
from intelliflow.core.application.core_application import ApplicationState
from intelliflow.mixins.aws.test_catalog import AllPassTestCatalog, AWSTestGlueCatalog


class AWSTestBase:
    testing_keyname = "testing"
    region = "us-east-1"
    # default moto acc id
    account_id = "123456789012"

    @pytest.fixture(scope="class")
    def aws_credentials(self):
        os.environ["AWS_ACCESS_KEY_ID"] = self.testing_keyname
        os.environ["AWS_SECRET_ACCESS_KEY"] = self.testing_keyname
        os.environ["AWS_SECURITY_TOKEN"] = self.testing_keyname
        os.environ["AWS_SESSION_TOKEN"] = self.testing_keyname
        # os.environ['MOTO_ACCOUNT_ID'] = self.account_id

    """Maintain RheocerOS' platform level interface with AWS here"""
    _aws_services = [
        mock_glue(),
        mock_events(),
        mock_dynamodb2(),
        mock_iam(),
        mock_sts(),
        mock_s3(),
        mock_sns(),
        mock_sqs(),
        mock_lambda(),
        mock_autoscaling(),
        mock_applicationautoscaling(),
        mock_kms(),
        mock_ses(),
        mock_athena(),
        mock_cloudwatch(),
    ]

    # COMPENSATE MOTO
    # BatchCompute
    _real_glue_create_job = compute_driver.create_glue_job
    _real_glue_get_glue_job = compute_driver.get_glue_job
    _real_glue_start_glue_job = compute_driver.start_glue_job
    _real_glue_delete_glue_job = compute_driver.delete_glue_job
    _real_compute_glue_catalog = compute_driver.glue_catalog
    _real_athena_glue_catalog = athena_compute_driver.glue_catalog
    _real_athena_glue_create_job = athena_compute_driver.create_glue_job
    _real_athena_glue_get_glue_job = athena_compute_driver.get_glue_job
    _real_athena_glue_start_glue_job = athena_compute_driver.start_glue_job
    _real_athena_glue_delete_glue_job = athena_compute_driver.delete_glue_job
    _real_athena_get_table_metadata = athena_compute_driver.get_table_metadata
    _real_athena_compute_query = athena_compute_driver.query
    _real_athena_create_or_update_workgroup = athena_compute_driver.create_or_update_workgroup
    _real_athena_delete_workgroup = athena_compute_driver.delete_workgroup
    _real_emr_glue_catalog = emr_compute_driver.glue_catalog
    #
    _real_auto_scaling_register_scalable_target = routing_driver.register_scalable_target
    _real_auto_scaling_deregister_scalable_target = routing_driver.deregister_scalable_target
    _real_auto_scaling_put_target_tracking_scaling_policy = routing_driver.put_target_tracking_scaling_policy
    _real_auto_scaling_delete_target_tracking_scaling_policy = routing_driver.delete_target_tracking_scaling_policy
    # processor
    _real_processor_invoke_lambda_function = processor_driver.invoke_lambda_function
    _real_processor_create_lambda_function = processor_driver.create_lambda_function
    _real_processor_put_function_concurrency = processor_driver.put_function_concurrency
    _real_processor_delete_function_concurrency = processor_driver.delete_function_concurrency
    _real_processor_update_lambda_function_conf = processor_driver.update_lambda_function_conf
    _real_processor_update_lambda_function_code = processor_driver.update_lambda_function_code
    _real_processor_add_permission = processor_driver.add_permission
    _real_processor_remove_permission = processor_driver.remove_permission
    _real_processor_glue_catalog = processor_driver.glue_catalog
    _real_storage_add_permission = storage_driver.add_permission
    _real_storage_remove_permission = storage_driver.remove_permission
    _real_diagnostics_add_permission = diagnostics_driver.add_permission
    _real_diagnostics_remove_permission = diagnostics_driver.remove_permission
    _real_diagnostics_put_composite_alarm = diagnostics_driver.put_composite_alarm
    # Application layer
    _real_external_nodes_glue_catalog = external_nodes.glue_catalog

    # signal_processing
    _real_routing_runtime_constructs_glue_catalog = routing_runtime_constructs.glue_catalog

    @pytest.fixture(scope="class")
    def glue_client(self, aws_credentials):
        with mock_glue():
            yield boto3.client(service_name="glue", region_name=self.region)

    @pytest.fixture(scope="class")
    def athena_client(self, aws_credentials):
        with mock_athena():
            yield boto3.client(service_name="athena", region_name=self.region)

    @pytest.fixture(scope="class")
    def cloudwatch_client(self, aws_credentials):
        with mock_cloudwatch():
            yield boto3.client(service_name="cloudwatch", region_name=self.region)

    @pytest.fixture(scope="class")
    def events_client(self, aws_credentials):
        with mock_events():
            yield boto3.client(service_name="events", region_name=self.region)

    @pytest.fixture(scope="class")
    def ddb_resource(self, aws_credentials):
        with mock_dynamodb2():
            yield boto3.resource(service_name="dynamodb", region_name=self.region)

    @pytest.fixture(scope="class")
    def s3_resource(self, aws_credentials):
        with mock_s3():
            yield boto3.resource(service_name="s3", region_name=self.region)

    @pytest.fixture(scope="class")
    def sns_resource(self, aws_credentials):
        with mock_sns():
            yield boto3.client(service_name="sns", region_name=self.region)

    def patch_aws_start(
        self,
        emulate_lambda=False,
        invoke_lambda_function_mock=None,
        glue_job_exists=False,
        glue_catalog_has_all_tables=False,
        glue_catalog: Optional[AWSTestGlueCatalog] = None,
    ):
        for service in self._aws_services:
            service.start()

        self._patch_glue_start(glue_job_exists)
        self._patch_athena_start()
        self._patch_auto_scaling_start()
        self._patch_cloudwatch_start()
        if not emulate_lambda:
            self._patch_lambda_start(invoke_lambda_function_mock)

        storage_driver.add_permission = MagicMock()
        storage_driver.remove_permission = MagicMock()
        diagnostics_driver.add_permission = MagicMock()
        diagnostics_driver.remove_permission = MagicMock()

        if glue_catalog_has_all_tables:
            allpass_catalog = AllPassTestCatalog()
            processor_driver.glue_catalog = allpass_catalog
            compute_driver.glue_catalog = allpass_catalog
            athena_compute_driver.glue_catalog = allpass_catalog
            emr_compute_driver.glue_catalog = allpass_catalog
            external_nodes.glue_catalog = allpass_catalog
            routing_runtime_constructs.glue_catalog = allpass_catalog
        elif glue_catalog:
            processor_driver.glue_catalog = glue_catalog
            compute_driver.glue_catalog = glue_catalog
            athena_compute_driver.glue_catalog = glue_catalog
            emr_compute_driver.glue_catalog = glue_catalog
            external_nodes.glue_catalog = glue_catalog
            routing_runtime_constructs.glue_catalog = glue_catalog

        # orchestration local message loop
        self._processor_thread: threading.Thread = None
        self._processor_thread_mutex: threading.RLock = None
        self._processor_thread_running: bool = False
        self._cycle_time_in_secs: int = None

        self._routingtable_route_mutex_map: Dict[routing_runtime_constructs.RouteID, threading.RLock] = dict()

    def activate_event_propagation(self, app: "Application", cycle_time_in_secs: int = 60):
        if self._processor_thread:
            raise ValueError(f"Event propagation is already active for app: {app.id}!")

        def invoke_lambda_function(lambda_client, function_name, function_params, is_async=True):
            """Synchronize internal async lambda invocations so that the chaining would be active during testing."""
            return app.platform.processor.process(function_params, use_activated_instance=False)

        processor_driver.invoke_lambda_function = MagicMock(side_effect=invoke_lambda_function)

        self._cycle_time_in_secs = cycle_time_in_secs

        def next_cycle(self, app):
            while self._processor_thread_running:
                try:
                    if (
                        app.state == ApplicationState.ACTIVE
                    ):  # so Application::pause and Application::resume can now be utilized during tests.
                        app.platform.routing_table.check_active_routes()

                        queued_events = app.platform.processor_queue.receive(90, 10)
                        if queued_events:
                            for event in queued_events:
                                # recursive call (use the current process context by use_activated_instance)
                                app.platform.processor.process(event, use_activated_instance=False)
                            app.platform.processor_queue.delete(queued_events)
                    time.sleep(self._cycle_time_in_secs)  # 60 secs = IF orchestration heartbeat rate
                except:
                    pass

        self._processor_thread = threading.Thread(target=next_cycle, args=(self, app))
        self._processor_thread_mutex = threading.RLock()
        self._processor_thread_running = True

        # patch Routing low-level sycnhronization to avoid concurrency issues in an impl agnostic way
        # (some of the impls for RoutingTable::_release and _lock might not be hard to emulate locally, e.g DDB locking)
        def _lock(route_id: "RouteID") -> None:
            with self._processor_thread_mutex:  # added because not sure about the atomicity of setdefault by GIL
                self._routingtable_route_mutex_map.setdefault(route_id, threading.RLock()).acquire()

        def _release(route_id: "RouteID") -> None:
            self._routingtable_route_mutex_map[route_id].release()

        app.platform.routing_table._release = MagicMock(side_effect=_release)
        app.platform.routing_table._lock = MagicMock(side_effect=_lock)

        self._processor_thread.start()

    def patch_aws_stop(self):
        if self._processor_thread:
            self._processor_thread_running = False
            self._processor_thread.join(timeout=self._cycle_time_in_secs)
            self._processor_thread = None

        for service in self._aws_services:
            service.stop()

        self._patch_glue_stop()
        self._patch_athena_stop()
        self._patch_auto_scaling_stop()
        self._patch_cloudwatch_stop()
        self._patch_lambda_stop()

        processor_driver.glue_catalog = AWSTestBase._real_processor_glue_catalog
        compute_driver.glue_catalog = AWSTestBase._real_compute_glue_catalog
        athena_compute_driver.glue_catalog = AWSTestBase._real_athena_glue_catalog
        emr_compute_driver.glue_catalog = AWSTestBase._real_emr_glue_catalog

        storage_driver.add_permission = AWSTestBase._real_storage_add_permission
        storage_driver.remove_permission = AWSTestBase._real_storage_remove_permission
        diagnostics_driver.add_permission = AWSTestBase._real_diagnostics_add_permission
        diagnostics_driver.remove_permission = AWSTestBase._real_diagnostics_remove_permission

        external_nodes.glue_catalog = AWSTestBase._real_external_nodes_glue_catalog
        routing_runtime_constructs.glue_catalog = AWSTestBase._real_routing_runtime_constructs_glue_catalog

    def _patch_lambda_start(self, invoke_lambda_function_mock=None):
        def create_lambda_function(
            lambda_client,
            function_name,
            description,
            handler_name,
            iam_role_arn,
            deployment_package,
            python_major_ver,
            python_minor_ver,
            dead_letter_target_arn,
            **kwargs,
        ):
            return f"arn:aws:lambda:{self.region}:{self.account_id}:function:{function_name}"

        processor_driver.create_lambda_function = MagicMock(side_effect=create_lambda_function)
        processor_driver.invoke_lambda_function = (
            MagicMock(side_effect=invoke_lambda_function_mock) if invoke_lambda_function_mock else MagicMock()
        )
        processor_driver.update_lambda_function_code = MagicMock()
        processor_driver.put_function_concurrency = MagicMock()
        processor_driver.delete_function_concurrency = MagicMock()
        processor_driver.update_lambda_function_conf = MagicMock()
        processor_driver.add_permission = MagicMock()
        processor_driver.remove_permission = MagicMock()

    def _patch_lambda_stop(self):
        processor_driver.invoke_lambda_function = AWSTestBase._real_processor_invoke_lambda_function
        processor_driver.create_lambda_function = AWSTestBase._real_processor_create_lambda_function
        processor_driver.put_function_concurrency = AWSTestBase._real_processor_put_function_concurrency
        processor_driver.delete_function_concurrency = AWSTestBase._real_processor_delete_function_concurrency
        processor_driver.update_lambda_function_conf = AWSTestBase._real_processor_update_lambda_function_conf
        processor_driver.update_lambda_function_code = AWSTestBase._real_processor_update_lambda_function_code
        processor_driver.add_permission = AWSTestBase._real_processor_add_permission
        processor_driver.remove_permission = AWSTestBase._real_processor_remove_permission

    def _patch_glue_start(self, job_exists: bool = False):
        def create_glue_job(*args, **kwargs):
            pass

        compute_driver.create_glue_job = MagicMock(side_effect=create_glue_job)

        def get_glue_job(glue_client, job_name):
            return job_name if job_exists else None

        compute_driver.get_glue_job = MagicMock(side_effect=get_glue_job)

        def start_glue_job(*args, **kwargs):
            pass

        compute_driver.start_glue_job = MagicMock(side_effect=start_glue_job)

        def delete_glue_job(*args, **kwargs):
            pass

        compute_driver.delete_glue_job = MagicMock(side_effect=delete_glue_job)

    def _patch_glue_stop(self):
        compute_driver.create_glue_job = AWSTestBase._real_glue_create_job
        compute_driver.get_glue_job = AWSTestBase._real_glue_get_glue_job
        compute_driver.start_glue_job = AWSTestBase._real_glue_start_glue_job
        compute_driver.delete_glue_job = AWSTestBase._real_glue_delete_glue_job

    def _patch_auto_scaling_start(self):
        routing_driver.register_scalable_target = MagicMock()
        routing_driver.deregister_scalable_target = MagicMock()
        routing_driver.put_target_tracking_scaling_policy = MagicMock()
        routing_driver.delete_target_tracking_scaling_policy = MagicMock()

    def _patch_auto_scaling_stop(self):
        routing_driver.register_scalable_target = AWSTestBase._real_auto_scaling_register_scalable_target
        routing_driver.deregister_scalable_target = AWSTestBase._real_auto_scaling_deregister_scalable_target
        routing_driver.put_target_tracking_scaling_policy = AWSTestBase._real_auto_scaling_put_target_tracking_scaling_policy
        routing_driver.delete_target_tracking_scaling_policy = AWSTestBase._real_auto_scaling_delete_target_tracking_scaling_policy

    def _patch_cloudwatch_start(self):
        diagnostics_driver.put_composite_alarm = MagicMock()

    def _patch_cloudwatch_stop(self):
        diagnostics_driver.put_composite_alarm = AWSTestBase._real_diagnostics_put_composite_alarm

    def _patch_athena_start(self):
        def query(*args, **kwargs):
            from uuid import uuid4

            return str(uuid4())

        athena_compute_driver.query = MagicMock(side_effect=query)

        def create_or_update_workgroup(*args, **kwargs):
            return

        athena_compute_driver.create_or_update_workgroup = MagicMock(side_effect=create_or_update_workgroup)

        def delete_workgroup(*args, **kwargs):
            return

        athena_compute_driver.delete_workgroup = MagicMock(side_effect=delete_workgroup)

        def create_glue_job(*args, **kwargs):
            pass

        athena_compute_driver.create_glue_job = MagicMock(side_effect=create_glue_job)

        def get_glue_job(glue_client, job_name):
            return None

        athena_compute_driver.get_glue_job = MagicMock(side_effect=get_glue_job)

        def start_glue_job(*args, **kwargs):
            pass

        athena_compute_driver.start_glue_job = MagicMock(side_effect=start_glue_job)

        def delete_glue_job(*args, **kwargs):
            pass

        athena_compute_driver.delete_glue_job = MagicMock(side_effect=delete_glue_job)

        def get_table_metadata(*args, **kwargs):
            return {
                "TableMetadata": {
                    "Columns": [{"Name": "foo1", "Type": "string", "Comment": ""}, {"Name": "foo2", "Type": "bigint", "Comment": ""}]
                }
            }

        athena_compute_driver.get_table_metadata = MagicMock(side_effect=get_table_metadata)

    def _patch_athena_stop(self):
        athena_compute_driver.query = AWSTestBase._real_athena_compute_query
        athena_compute_driver.create_or_update_workgroup = AWSTestBase._real_athena_create_or_update_workgroup
        athena_compute_driver.delete_workgroup = AWSTestBase._real_athena_delete_workgroup
        athena_compute_driver.create_glue_job = AWSTestBase._real_athena_glue_create_job
        athena_compute_driver.get_glue_job = AWSTestBase._real_athena_glue_get_glue_job
        athena_compute_driver.start_glue_job = AWSTestBase._real_athena_glue_start_glue_job
        athena_compute_driver.delete_glue_job = AWSTestBase._real_athena_glue_delete_glue_job
        athena_compute_driver.get_table_metadata = AWSTestBase._real_athena_get_table_metadata
