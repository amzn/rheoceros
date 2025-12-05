from datetime import datetime, timedelta

import pytest

from intelliflow.api_ext import *
from intelliflow.core.platform.constructs import (
    ConstructEncryption,
    ConstructPermission,
    ConstructPersistenceSecurityDef,
    ConstructSecurityConf,
    EncryptionKeyAllocationLevel,
    Storage,
)
from intelliflow.mixins.aws.test import AWSTestBase


class TestAWSApplicationExternalEntityAuth(AWSTestBase):
    def test_application_external_entity_auth_standalone(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        app = TestAWSApplicationExecutionControl._create_test_application(self, "ext_ent_app_1")

        entity_role = "arn:aws:iam::111222333444:role/ExternalTeamRole"
        with pytest.raises(ValueError):
            app.authorize_external_entity(entity_role, Storage, expiration_date=datetime.utcnow() - timedelta(days=1))

        app.authorize_external_entity(entity_role, Storage)

        # SERIALIZATION: inject serialize/deserialize sequence for enhanced serialization coverage
        json_str = app.dev_context.to_json()
        dev_context = CoreData.from_json(json_str)
        app._dev_context = dev_context
        #

        app.activate()

        # check the policy
        policy = app.platform.storage._bucket.Policy()
        policy.load()
        current_policy_doc = json.loads(policy.policy)
        current_statements = current_policy_doc["Statement"]
        assert any(
            (statement["Principal"]["AWS"] == entity_role or statement["Principal"]["AWS"] == [entity_role])
            for statement in current_statements
        )

        # activation is successful with external entity auth conf
        # now let's see if we can re-load
        app = AWSApplication("ext_ent_app_1", self.region)
        # this will reset the conf
        app.activate()

        # check the policy again for cleanup
        policy = app.platform.storage._bucket.Policy()
        policy.load()
        current_policy_doc = json.loads(policy.policy)
        current_statements = current_policy_doc["Statement"]
        assert not any(
            (statement["Principal"]["AWS"] == entity_role or statement["Principal"]["AWS"] == [entity_role])
            for statement in current_statements
        )

        self.patch_aws_stop()

    def test_application_external_entity_auth_unsupported_driver(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        app = TestAWSApplicationExecutionControl._create_test_application(self, "ext_ent_app_2")

        entity_role = "arn:aws:iam::111222333444:role/ExternalTeamRole"

        # this means "authorize on all components"
        # but if any of the underlying drivers does not support the operation, we let user know about it as early as
        # possible.
        app.authorize_external_entity(entity_role, None)

        with pytest.raises(NotImplementedError):
            app.activate()

        # even a valid auth will save activation from failing because previous authorization will still be rejected by
        # other drivers.
        app.authorize_external_entity(entity_role, Storage)
        with pytest.raises(NotImplementedError):
            app.activate()

        app = AWSApplication("ext_ent_app_2", self.region)

        # similarly RoutingTable does not support it as of (09/2024), so let's raise!
        app.authorize_external_entity(entity_role, RoutingTable)

        with pytest.raises(NotImplementedError):
            app.activate()

        self.patch_aws_stop()

    def test_application_external_entity_auth_multiple_entities(self):
        self.patch_aws_start(glue_catalog_has_all_andes_tables=True)

        from test.intelliflow.core.application.test_aws_application_execution_control import TestAWSApplicationExecutionControl

        app = TestAWSApplicationExecutionControl._create_test_application(self, "ext_ent_app_3")

        entity_role = "arn:aws:iam::111222333444:role/ExternalTeamRole"
        entity_role2 = f"arn:aws:iam::222333444555:role/ExternalTeamRole2"

        app.authorize_external_entity(
            entity_role, Storage, extra_permissions=[ConstructPermission([app.platform.storage.get_storage_resource_path()], ["s3:List*"])]
        )
        app.authorize_external_entity(
            entity_role, Storage, extra_permissions=[ConstructPermission([app.platform.storage.get_storage_resource_path()], ["s3:Get*"])]
        )
        app.authorize_external_entity(
            entity_role2, Storage, extra_permissions=[ConstructPermission([app.platform.storage.get_storage_resource_path()], ["s3:Get*"])]
        )

        app.activate()

        # check the policy
        policy = app.platform.storage._bucket.Policy()
        policy.load()
        current_policy_doc = json.loads(policy.policy)
        current_statements = current_policy_doc["Statement"]
        assert any(set(statement["Principal"]["AWS"]) == {entity_role, entity_role2} for statement in current_statements)

        self.patch_aws_stop()
