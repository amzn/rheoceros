# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import pytest

from intelliflow.core.platform.definitions.aws.glue import catalog as glue_catalog
from intelliflow.core.platform.definitions.aws.glue.catalog import *
from intelliflow.mixins.aws.test import AWSTestBase


class TestCatalogClientWrapper(AWSTestBase):
    glue_event_rule = "glue_event_rule"
    lambda_func_name = "test-func"
    lambda_func_arn = "arn:aws:lambda:us-west-2:123456789012:function:test-func"
    test_glue_db = "mock-db"
    test_glue_table = "mock-table"

    @pytest.fixture(scope="class")
    def setup_glue_resources(self, glue_client):
        glue_client.create_database(DatabaseInput={"Name": self.test_glue_db})
        glue_client.create_table(DatabaseName=self.test_glue_db, TableInput={"Name": self.test_glue_table})

    def test_check_table_exist(self, setup_glue_resources):
        assert glue_catalog.check_table(boto3.Session(), self.region, self.test_glue_db, self.test_glue_table) == True

    def test_check_table_not_exist(self, setup_glue_resources):
        assert glue_catalog.check_table(boto3.Session(), self.region, self.test_glue_db, "not" + self.test_glue_table) == False

    def test_check_table_in_non_existent_db_raise_exception(self, setup_glue_resources):
        with pytest.raises(Exception) as error:
            glue_catalog.check_table(boto3.Session(), self.region, "not" + self.test_glue_db, self.test_glue_table)
        assert error.typename == "EntityNotFoundException"

    def test_provision_cw_event_rule(self, events_client):
        response = provision_cw_event_rule(boto3.Session(), self.region, self.glue_event_rule, self.lambda_func_name, self.lambda_func_arn)
        assert response.cw_event_arn.__contains__(self.glue_event_rule)
