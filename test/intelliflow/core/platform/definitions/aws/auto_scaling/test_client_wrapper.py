# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import boto3
import botocore
import pytest
from botocore import client
from botocore.exceptions import ClientError
from mock import ANY, MagicMock

from intelliflow.core.platform.definitions.aws.auto_scaling.client_wrapper import (
    deregister_scalable_target,
    put_target_tracking_scaling_policy,
    register_scalable_target,
)
from intelliflow.mixins.aws.test import AWSTestBase


class TestClientWrapperForAutoScaling(AWSTestBase):
    @pytest.fixture()
    def mock_auto_scaling_client(self, monkeypatch):
        auto_scaling_client = MagicMock()
        return auto_scaling_client

    @pytest.fixture()
    def mock_register_scalable_target(self, monkeypatch, mock_auto_scaling_client):
        mock_register_scalable_target = MagicMock()
        monkeypatch.setattr(mock_auto_scaling_client, "register_scalable_target", mock_register_scalable_target)
        return mock_register_scalable_target

    @pytest.fixture()
    def mock_register_scalable_target_exception(self, monkeypatch, mock_auto_scaling_client):
        error = ClientError(operation_name="op", error_response={"Error": {}})
        mock_register_scalable_target = MagicMock(side_effect=error)
        monkeypatch.setattr(mock_auto_scaling_client, "register_scalable_target", mock_register_scalable_target)
        return mock_register_scalable_target

    @pytest.fixture()
    def mock_deregister_scalable_target(self, monkeypatch, mock_auto_scaling_client):
        mock_deregister_scalable_target = MagicMock()
        monkeypatch.setattr(mock_auto_scaling_client, "deregister_scalable_target", mock_deregister_scalable_target)
        return mock_deregister_scalable_target

    @pytest.fixture()
    def mock_deregister_scalable_target_exception(self, monkeypatch, mock_auto_scaling_client):
        error = ClientError(operation_name="op", error_response={"Error": {}})
        mock_deregister_scalable_target = MagicMock(side_effect=error)
        monkeypatch.setattr(mock_auto_scaling_client, "deregister_scalable_target", mock_deregister_scalable_target)
        return mock_deregister_scalable_target

    @pytest.fixture()
    def mock_target_tracking(self, monkeypatch, mock_auto_scaling_client):
        mock_target_tracking = MagicMock()
        monkeypatch.setattr(mock_auto_scaling_client, "put_scaling_policy", mock_target_tracking)
        return mock_target_tracking

    @pytest.fixture()
    def mock_target_tracking_exception(self, monkeypatch, mock_auto_scaling_client):
        error = ClientError(operation_name="op", error_response={"Error": {}})
        mock_target_tracking = MagicMock(side_effect=error)
        monkeypatch.setattr(mock_auto_scaling_client, "put_scaling_policy", mock_target_tracking)
        return mock_target_tracking

    def test_as_register_resource(self, mock_auto_scaling_client, mock_register_scalable_target):
        register_scalable_target(
            auto_scaling_client=mock_auto_scaling_client,
            service_name="dynamodb",
            resource_id="table/{}".format("testTable"),
            scalable_dimension="dynamodb:table:ReadCapacityUnits",
            min_capacity=5,
            max_capacity=500,
        )
        assert mock_register_scalable_target.call_count == 1

    def test_as_register_resource_exception(self, mock_auto_scaling_client, mock_register_scalable_target_exception):
        with pytest.raises(Exception) as error:
            register_scalable_target(
                auto_scaling_client=mock_auto_scaling_client,
                service_name="dynamodb",
                resource_id="table/{}".format("testTable"),
                scalable_dimension="dynamodb:table:ReadCapacityUnits",
                min_capacity=5,
                max_capacity=500,
            )
        assert error.typename == "ClientError"

    def test_as_deregister_resource(self, mock_auto_scaling_client, mock_deregister_scalable_target):
        deregister_scalable_target(
            auto_scaling_client=mock_auto_scaling_client,
            service_name="dynamodb",
            resource_id="table/{}".format("testTable"),
            scalable_dimension="dynamodb:table:ReadCapacityUnits",
        )
        assert mock_deregister_scalable_target.call_count == 1

    def test_as_deregister_resource_exception(self, mock_auto_scaling_client, mock_deregister_scalable_target_exception):
        with pytest.raises(Exception) as error:
            deregister_scalable_target(
                auto_scaling_client=mock_auto_scaling_client,
                service_name="dynamodb",
                resource_id="table/{}".format("testTable"),
                scalable_dimension="dynamodb:table:ReadCapacityUnits",
            )
        assert error.typename == "ClientError"

    def test_as_target_tracking_scaling_policy(self, mock_auto_scaling_client, mock_target_tracking):
        put_target_tracking_scaling_policy(
            auto_scaling_client=mock_auto_scaling_client,
            service_name="dynamodb",
            resource_id="table/{}".format("testTable"),
            scalable_dimension="dynamodb:table:ReadCapacityUnits",
            policy_dict={"PolicyName": "Name", "PolicyConfig": "Config"},
        )
        assert mock_target_tracking.call_count == 1

    def test_as_target__tracking_scaling_policy_exception(self, mock_auto_scaling_client, mock_target_tracking_exception):
        with pytest.raises(Exception) as error:
            put_target_tracking_scaling_policy(
                auto_scaling_client=mock_auto_scaling_client,
                service_name="dynamodb",
                resource_id="table/{}".format("testTable"),
                scalable_dimension="dynamodb:table:ReadCapacityUnits",
                policy_dict={"PolicyName": "Name", "PolicyConfig": "Config"},
            )
        assert error.typename == "ClientError"
