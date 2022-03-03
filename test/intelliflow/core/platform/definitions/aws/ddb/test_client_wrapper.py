# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest
from boto3.dynamodb.conditions import Key

from intelliflow.core.platform.definitions.aws.ddb.client_wrapper import (
    create_table,
    delete_ddb_item,
    delete_table,
    get_ddb_item,
    put_ddb_item,
    put_item_batch,
    query_ddb_table,
    scan_ddb_table,
)
from intelliflow.mixins.aws.test import AWSTestBase


class TestClientWrapperForDDB(AWSTestBase):
    ddb_table_name = "TestDDBTable"
    key_schema = [{"AttributeName": "route_id", "KeyType": "HASH"}]

    attribute_def = [{"AttributeName": "route_id", "AttributeType": "S"}]

    provisioned_throughput = {"ReadCapacityUnits": 20, "WriteCapacityUnits": 20}

    item = {"route_id": "route55", "random_attr": "attr_val"}
    key = {"route_id": "route55"}

    item_for_batch = {"route_id": "route75", "random_attr": "attr_val"}
    key_for_batch = {"route_id": "route75"}

    item_to_delete = {"route_id": "route66"}
    key_to_delete = {"route_id": "route66"}

    key_non_existent = {"route_id": "route56"}

    @pytest.fixture(scope="class")
    def setup_ddb_table(self, ddb_resource):
        ddb_table = create_table(ddb_resource, self.ddb_table_name, self.key_schema, self.attribute_def, self.provisioned_throughput)

        put_ddb_item(ddb_table, self.item)
        put_ddb_item(ddb_table, self.item_to_delete)
        yield ddb_table

    def test_ddb_create_table(self, ddb_resource, setup_ddb_table):
        ddb_table = setup_ddb_table

        assert ddb_table.table_name == self.ddb_table_name

    def test_ddb_catch_create_same_table_exception(self, ddb_resource, setup_ddb_table):
        with pytest.raises(Exception) as error:
            create_table(ddb_resource, self.ddb_table_name, self.key_schema, self.attribute_def, self.provisioned_throughput)
        assert error.typename == "ResourceInUseException"

    def test_ddb_delete_table(self, ddb_resource):
        table_name = "TestDDBTableDeleteOp"
        table = create_table(ddb_resource, table_name, self.key_schema, self.attribute_def, self.provisioned_throughput)
        response = delete_table(table)
        assert response["TableDescription"]["TableName"] == table_name

    def test_ddb_catch_delete_non_existent_table_exception(self, ddb_resource):
        table = ddb_resource.Table("TableNE")
        with pytest.raises(Exception) as error:
            delete_table(table)
        assert error.typename == "ResourceNotFoundException"

    def test_ddb_get_item(self, ddb_resource, setup_ddb_table):
        table = setup_ddb_table
        response = get_ddb_item(table, self.key)
        assert response["Item"] == self.item

    def test_ddb_delete_item(self, ddb_resource, setup_ddb_table):
        table = setup_ddb_table
        response = delete_ddb_item(table, self.key_to_delete)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_ddb_query(self, ddb_resource, setup_ddb_table):
        table = setup_ddb_table
        key_cond_expr = Key("route_id").eq("route55")
        scan_index_forward = True
        response = query_ddb_table(table, key_cond_expr, scan_index_forward)
        assert response["Items"][0] == self.item

    def test_ddb_scan(self, ddb_resource, setup_ddb_table):
        table = setup_ddb_table
        response = scan_ddb_table(table)
        assert response["Items"][0] == self.item

    def test_ddb_batch_writer_put_item(self, setup_ddb_table):
        table = setup_ddb_table
        with table.batch_writer(overwrite_by_pkeys=["route_id"]) as batch:
            put_item_batch(batch, self.item_for_batch)

        response = get_ddb_item(table, self.key_for_batch)
        assert response["Item"] == self.item_for_batch
