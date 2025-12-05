# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from test.intelliflow.core.platform.driver_test_utils import DriverTestUtils
from unittest.mock import MagicMock

import boto3
import pytest

from intelliflow.core.platform.constructs import RoutingActivationStrategy, RoutingTable
from intelliflow.core.platform.definitions.aws.common import CommonParams
from intelliflow.core.platform.definitions.compute import (
    ComputeResourceDesc,
    ComputeSessionDesc,
    ComputeSuccessfulResponse,
    ComputeSuccessfulResponseType,
)
from intelliflow.core.platform.development import AWSConfiguration, DevelopmentPlatform, HostPlatform
from intelliflow.core.platform.drivers.routing.aws import AWSDDBRoutingTable
from intelliflow.core.signal_processing import Slot
from intelliflow.core.signal_processing.definitions.compute_defs import ABI, Lang
from intelliflow.core.signal_processing.slot import SlotType
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSRoutingTableDriver(AWSTestBase, DriverTestUtils):
    def setup_platform_and_params(self):
        self.params = {}
        self.init_common_utils()
        self.params[CommonParams.BOTO_SESSION] = boto3.Session(None, None, None, self.region)
        self.params[CommonParams.REGION] = self.region
        self.params[CommonParams.ACCOUNT_ID] = self.account_id

    def get_driver_and_platform(self):
        mock_platform = HostPlatform(
            AWSConfiguration.builder()
            .with_default_credentials(as_admin=True)
            .with_region("us-east-1")
            .with_routing_table(AWSDDBRoutingTable)
            .build()
        )
        # Prevent intermittent test failures due to platform state loading during context_id assignment
        mock_platform.should_load_constructs = lambda: False
        return (
            AWSDDBRoutingTable(self.params),
            mock_platform,
        )

    def setup_test_route_record(self, mock_routing_table, mock_route=None, deactivation_timestamp=None, compute_resp=None):
        if mock_route is None:
            self.test_route = self.route_1_basic
        else:
            self.test_route = mock_route

        self.test_route_record = RoutingTable.RouteRecord(self.test_route.clone())
        compute_record = RoutingTable.ComputeRecord(
            1608154549,
            [self.signal_internal_1],
            self.test_route.slots[0],
            self.test_route.output,
            "8fd9995c-af42-4cf2-a2ca-e62eacce44f0",
            deactivated_timestamp_utc=deactivation_timestamp,
            state=compute_resp,
        )
        self.test_route_record.add_active_compute_record(compute_record)
        mock_routing_table._save(self.test_route_record)

    def check_route_record_equality(self, route_record_1: RoutingTable.RouteRecord, route_record_2: RoutingTable.RouteRecord):
        if (route_record_1.route == route_record_2.route) and (
            route_record_1.active_compute_records == route_record_2.active_compute_records
        ):
            return True
        return False

    def test_mock_ddbroutingtable_successful_activate(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_routing_table, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_ddb_1"
        mock_routing_table.dev_init(mock_host_platform)
        mock_routing_table.activate()
        self.setup_test_route_record(mock_routing_table)
        saved_record = mock_routing_table._load(self.test_route_record.route.route_id)
        assert self.check_route_record_equality(self.test_route_record, saved_record)

        assert mock_routing_table.routing_table_name == AWSDDBRoutingTable.ROUTES_TABLE_NAME_FORMAT.format(
            AWSDDBRoutingTable.__name__, mock_host_platform._context_id.lower(), self.params[CommonParams.REGION]
        )
        assert mock_routing_table.routing_history_table_name == AWSDDBRoutingTable.ROUTES_HISTORY_TABLE_NAME_FORMAT.format(
            AWSDDBRoutingTable.__name__, mock_host_platform._context_id.lower(), self.params[CommonParams.REGION]
        )
        self.patch_aws_stop()

    def test_mock_ddbroutingtable_table_name_len_exception_activate(self, monkeypatch):
        self.patch_aws_start()
        with pytest.raises(Exception) as error:
            self.setup_platform_and_params()
            mock_routing_table, mock_host_platform = self.get_driver_and_platform()
            context_id = str()
            for c in range(257):
                context_id += str(c)
            mock_host_platform._context_id = context_id

            mock_routing_table.dev_init(mock_host_platform)

            mock_routing_table.activate()

        assert error.typename == "ValueError"
        self.patch_aws_stop()

    def test_mock_ddbroutingtable_successful_activate_twice(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_routing_table, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_ddb_2"
        mock_routing_table.dev_init(mock_host_platform)
        mock_routing_table.activate()
        self.setup_test_route_record(mock_routing_table)
        mock_routing_table.activate()
        saved_record = mock_routing_table._load(self.test_route_record.route.route_id)
        assert self.check_route_record_equality(self.test_route_record, saved_record)
        self.patch_aws_stop()

    def test_mock_ddbroutingtable_sucessful_activate_twice_clear_all_routes(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_routing_table, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_ddb_3"
        mock_routing_table.dev_init(mock_host_platform)
        mock_routing_table.activate()
        self.setup_test_route_record(mock_routing_table)
        mock_routing_table.activation_strategy = RoutingActivationStrategy.ALWAYS_CLEAR_ACTIVE_ROUTES
        mock_routing_table.activate()
        saved_record = mock_routing_table._load(self.test_route_record.route.route_id)
        assert saved_record is None
        self.patch_aws_stop()

    def test_mock_ddbroutingtable_successful_fetch_active_route_record(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_routing_table, mock_host_platform = self.get_driver_and_platform()
        mock_host_platform._context_id = "test123_ddb_4"
        mock_routing_table.dev_init(mock_host_platform)
        mock_routing_table.activate()
        self.setup_test_route_record(mock_routing_table)
        route_record_iterator = mock_routing_table.load_active_route_records()
        fetched_route_record = next(route_record_iterator)
        diff = self.test_route_record.active_compute_records.difference(fetched_route_record.active_compute_records)
        assert len(diff) == 0
        self.patch_aws_stop()

    def test_mock_ddbroutingtable_successful_process_signals_with_new_signal(self):
        self.patch_aws_start()
        self.setup_platform_and_params()
        mock_routing_table, mock_host_platform = self.get_driver_and_platform()

        mock_host_platform._context_id = "test123_ddb_5"
        mock_routing_table.dev_init(mock_host_platform)
        mock_routing_table.activate()

        old_route = self.route_1_basic
        compute_response = ComputeSuccessfulResponse(
            ComputeSuccessfulResponseType.PROCESSING, ComputeSessionDesc("job_id", ComputeResourceDesc("job_name", "job_arn"))
        )
        self.setup_test_route_record(
            mock_routing_table, mock_route=old_route, deactivation_timestamp=1608154579, compute_resp=compute_response
        )

        new_route = self.test_route.clone()
        new_slot = Slot(SlotType.ASYNC_BATCH_COMPUTE, "output = input1", Lang.PYTHON, ABI.GLUE_EMBEDDED, {"MaxCapacity": 100}, None)
        new_route._slots = [new_slot]
        mock_routing_table._process_internal({old_route}, {new_route})
        assert not mock_routing_table.load_active_route_record(old_route.route_id)
        inactive_route_records_iterator = mock_routing_table.load_inactive_compute_records(old_route.route_id)
        fetched_inactive_record = next(inactive_route_records_iterator)
        diff = {fetched_inactive_record}.difference(self.test_route_record.active_compute_records)
        assert len(diff) == 0
        self.patch_aws_stop()
