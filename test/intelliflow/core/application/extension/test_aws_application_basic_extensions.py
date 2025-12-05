# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from intelliflow.api_ext import *
from intelliflow.core.platform.definitions.aws.ddb.client_wrapper import BillingMode
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSApplicationBasicExtensions(AWSTestBase):
    def test_application_extensions_zero_configuration(self):
        self.patch_aws_start()

        app = AWSApplication("extensions", self.region)

        # CompositeExtension should be there
        assert app.platform.extensions
        # however map should be empty
        assert not app.platform.extensions.extensions_map

        # check metrics
        system_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)
        extensions_metrics_map = system_metrics_map[CompositeExtension]

        assert not extensions_metrics_map, "There should not be any system metrics from platform.extensions!"

        self.patch_aws_stop()

    def test_application_extensions_basic_dynamodb_table(self):
        self.patch_aws_start()

        # CORNER CASES

        # extension_id must be provided
        with pytest.raises(AssertionError):
            DynamoDBTable(extension_id=None, table_name="my_extension_table")

        extension_id: str = "dynamodb_ext_1"

        # table_name must be defined once
        with pytest.raises(ValueError):
            DynamoDBTable(extension_id=extension_id, table_name="my_table", TableName="my_table")

        # provisioned mode now supported
        DynamoDBTable(extension_id=extension_id, table_name="my_table", BillingMode=BillingMode.PROVISIONED)

        my_table = DynamoDBTable(extension_id=extension_id, table_name="my_extension_table")

        app = AWSApplication("extensions", self.region, PLATFORM_EXTENSIONS=[my_table])

        # CompositeExtension should be there
        assert app.platform.extensions
        # map should not be empty
        assert app.platform.extensions.extensions_map

        assert app.platform.extensions[extension_id]
        provisioned_table_name = app.platform.extensions[extension_id].table_name
        assert provisioned_table_name == "my_extension_table"

        # check metrics
        system_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)
        extensions_metrics_map = system_metrics_map[CompositeExtension]

        assert extensions_metrics_map, "There must be system metrics from the platform extension!"

        table_metric_signal = extensions_metrics_map[extension_id + "Table"]

        metric_that_can_be_an_alarmn_input = table_metric_signal["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(15)]
        assert metric_that_can_be_an_alarmn_input

        # should not exist!
        import boto3

        ddb_client = boto3.client("dynamodb", region_name=self.region)
        try:
            ddb_client.describe_table(TableName=provisioned_table_name)
            assert False, "Extension table should not exist before the activation!"
        except ddb_client.exceptions.ResourceNotFoundException:
            # expected to be missing
            pass

        app.activate()

        # should exist!
        try:
            response = ddb_client.describe_table(TableName=provisioned_table_name)
            assert response
        except ddb_client.exceptions.ResourceNotFoundException:
            assert True, "Extension table should exist post activation!"

        try:
            response = ddb_client.describe_time_to_live(TableName=provisioned_table_name)
            assert response["TimeToLiveDescription"]["TimeToLiveStatus"] == "DISABLED"
        except ddb_client.exceptions.ResourceNotFoundException:
            pass

        # check update logic (e.g should call DDB update table)
        app.activate()

        app.terminate()

        # should not exist when the extention resources are deleted during termination
        try:
            ddb_client.describe_table(TableName=provisioned_table_name)
            assert True, "Extension table should not exist post termination!"
        except ddb_client.exceptions.ResourceNotFoundException:
            # expected to be missing
            pass

        self.patch_aws_stop()

    def test_application_extensions_basic_dynamodb_table_removal(self):
        self.patch_aws_start()

        extension_id: str = "dynamodb_ext_2"
        table_name = "auto_removal_table"
        my_table = DynamoDBTable(extension_id=extension_id, table_name=table_name)

        app = AWSApplication("extensions", self.region, PLATFORM_EXTENSIONS=[my_table])
        app.activate()

        # should exist!
        import boto3

        ddb_client = boto3.client("dynamodb", region_name=self.region)
        try:
            response = ddb_client.describe_table(TableName=table_name)
            assert response
        except ddb_client.exceptions.ResourceNotFoundException:
            assert True, "Extension table should exist post activation!"

        # in another session app is initiated without platform extensions
        # we expect the app to remove the extension and call termination on it
        app = AWSApplication("extensions", self.region)
        app.activate()

        # should not exist when the extention resources are deleted during termination
        try:
            ddb_client.describe_table(TableName=table_name)
            assert True, "Extension table should not exist post termination!"
        except ddb_client.exceptions.ResourceNotFoundException:
            # expected to be missing
            pass

        self.patch_aws_stop()

    def test_application_extensions_basic_multiple_configurations_dynamodb_table(self):
        self.patch_aws_start()

        # same extension_id
        with pytest.raises(ValueError):
            app = AWSApplication(
                "ext-multi-ddb",
                self.region,
                PLATFORM_EXTENSIONS=[
                    DynamoDBTable(extension_id="ext1", table_name="my_extension_table"),
                    DynamoDBTable(extension_id="ext1", table_name="my_table"),
                ],
            )

        # same table_name (DynamoDB extension's own logic)
        with pytest.raises(ValueError):
            app = AWSApplication(
                "ext-multi-ddb",
                self.region,
                PLATFORM_EXTENSIONS=[
                    DynamoDBTable(extension_id="ext1", table_name="my_extension_table"),
                    DynamoDBTable(extension_id="ext2", table_name="my_extension_table"),
                ],
            )

        app = AWSApplication(
            "ext-multi-ddb",
            self.region,
            PLATFORM_EXTENSIONS=[
                DynamoDBTable(extension_id="ext1"),  # let the extension name the table
                DynamoDBTable(extension_id="ext2", table_name="my_table2"),
            ],
        )

        # CompositeExtension should be there
        assert app.platform.extensions
        # map should not be empty
        assert len(app.platform.extensions.extensions_map) == 2

        # check table names
        # 1- extension determined default name
        ext1 = app.platform.extensions["ext1"]
        ext1_default_table_name = ext1.generate_default_table_name(ext1.desc.extension_id, app.id, self.region)
        assert ext1.table_name == ext1_default_table_name

        ext2 = app.platform.extensions["ext2"]
        assert ext2.table_name == "my_table2"

        # check metrics
        system_metrics_map = app.get_platform_metrics(HostPlatform.MetricType.SYSTEM)
        extensions_metrics_map = system_metrics_map[CompositeExtension]

        assert extensions_metrics_map, "There must be system metrics from the platform extension!"

        table_metric_signal1 = extensions_metrics_map["ext1" + "Table"]
        table_metric_signal2 = extensions_metrics_map["ext2" + "Table"]

        assert table_metric_signal1["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(15)]
        assert table_metric_signal2["WriteThrottleEvents"][MetricStatistic.SUM][MetricPeriod.MINUTES(15)]

        app.activate()

        import boto3

        ddb_client = boto3.client("dynamodb", region_name=self.region)
        # both must exist!
        try:
            ddb_client.describe_table(TableName=ext1_default_table_name)
            ddb_client.describe_table(TableName=ext2.table_name)
        except ddb_client.exceptions.ResourceNotFoundException:
            assert True, "Extension tables should exist post activation!"

        # replace the second extension by changing the table_name
        app = AWSApplication(
            "ext-multi-ddb",
            self.region,
            PLATFORM_EXTENSIONS=[
                DynamoDBTable(extension_id="ext1"),  # let the extension name the table
                # will be treated as a new Extension (not a mere update on the previous Extension object for "ext2")
                DynamoDBTable(extension_id="ext2", table_name="my_table3"),
            ],
        )

        # till activation, previous ext2 should be kepts as a "replaced" driver/extension
        # and should go through termination during the activation
        assert app.platform.extensions
        # map should not be empty
        assert len(app.platform.extensions.extensions_map) == 2

        ext2 = app.platform.extensions["ext2"]
        assert ext2.table_name == "my_table3"

        assert len(app.platform.extensions.removed_extensions) == 1
        assert app.platform.extensions.removed_extensions[0].table_name == "my_table2"

        # "terminate" callback must be dispatched on the removed extension ("my_table2")
        app.activate()

        try:
            ddb_client.describe_table(TableName=ext1_default_table_name)
            ddb_client.describe_table(TableName=ext2.table_name)
        except ddb_client.exceptions.ResourceNotFoundException:
            assert True, "Extension tables should exist post activation!"

        # check the "removed" table
        try:
            ddb_client.describe_table(TableName="my_table2")
            assert True, "Removed extension table should not exist post activation!"
        except ddb_client.exceptions.ResourceNotFoundException:
            # expected to be missing
            pass

        app.terminate()

        # should not exist when the extention resources are deleted during termination
        try:
            ddb_client.describe_table(TableName=ext1_default_table_name)
            assert True, "Extension table should not exist post termination!"
        except ddb_client.exceptions.ResourceNotFoundException:
            # expected to be missing
            pass

        assert ext2.table_name is None
        try:
            ddb_client.describe_table(TableName="my_table3")
            assert True, "Extension table should not exist post termination!"
        except ddb_client.exceptions.ResourceNotFoundException:
            # expected to be missing
            pass

        self.patch_aws_stop()

    def test_application_extensions_basic_dynamodb_table_with_ttl(self):
        self.patch_aws_start()

        extension_id: str = "dynamodb_ext_ttl"
        ttl_attr = "creationTime"

        my_table = DynamoDBTable(extension_id=extension_id, table_name="my_extension_table", ttl_attribute_name=ttl_attr)

        app = AWSApplication("exts_ttl", self.region, PLATFORM_EXTENSIONS=[my_table])

        # CompositeExtension should be there
        assert app.platform.extensions
        # map should not be empty
        assert app.platform.extensions.extensions_map

        assert app.platform.extensions[extension_id]
        provisioned_table_name = app.platform.extensions[extension_id].table_name
        assert provisioned_table_name == "my_extension_table"

        app.activate()

        import boto3

        ddb_client = boto3.client("dynamodb", region_name=self.region)
        # first check if the table exists
        try:
            response = ddb_client.describe_table(TableName=provisioned_table_name)
            assert response
        except ddb_client.exceptions.ResourceNotFoundException:
            assert False, "Extension table should exist post activation!"

        response = ddb_client.describe_time_to_live(TableName=provisioned_table_name)
        assert response and response["TimeToLiveDescription"]["AttributeName"] == ttl_attr

        # UPDATE TTL ATTR
        ttl_attr = "creationTime2"
        my_table = DynamoDBTable(extension_id=extension_id, table_name="my_extension_table", ttl_attribute_name=ttl_attr)
        app = AWSApplication("exts_ttl", self.region, PLATFORM_EXTENSIONS=[my_table])
        app.activate()

        response = ddb_client.describe_time_to_live(TableName=provisioned_table_name)
        assert (
            response
            and response["TimeToLiveDescription"]["AttributeName"] == ttl_attr
            and response["TimeToLiveDescription"]["AttributeName"] == ttl_attr
        )

        # DISABLE TTL
        my_table = DynamoDBTable(extension_id=extension_id, table_name="my_extension_table", ttl_attribute_name=None)
        app = AWSApplication("exts_ttl", self.region, PLATFORM_EXTENSIONS=[my_table])
        app.activate()

        response = ddb_client.describe_time_to_live(TableName=provisioned_table_name)
        assert response and response["TimeToLiveDescription"]["AttributeName"] == ttl_attr

        app.terminate()

        self.patch_aws_stop()
